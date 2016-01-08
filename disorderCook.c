/* Crazy attempt to write the disorderBook backend in C.
   The data layout stolen from DanielVF.
   
   We store all data in memory so that the user can retrieve it later via
   yet-to-be-written functions. As such, there are very few free() calls.
   
   */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUY 1       // Don't change these now, they are also used in the frontend
#define SELL 2

#define LIMIT 1     // Don't change these now, they are also used in the frontend
#define MARKET 2
#define FOK 3
#define IOC 4

#define MAXINPUT 2048
#define MAXTOKENSIZE 100
#define MAXTOKENS 50                // Well-behaved frontend will never send this many

#define MAXORDERS 2000000000        // Not going all the way to MAX_INT, because various numbers might go above this
                  
#define TOO_MANY_ORDERS 1


typedef struct Fill_struct {
    int price;
    int qty;
    char * ts;
} FILL;

typedef struct FillNode_struct {
    struct Fill_struct * fill;
    struct FillNode_struct * prev;
    struct FillNode_struct * next;
} FILLNODE;

typedef struct AccountName_struct {             // It's easier to think about pointers to structs
    char name[MAXTOKENSIZE];                    // that have strings, than pointers to strings.
} ACCOUNT;

typedef struct Order_struct {
    int direction;
    int originalQty;
    int qty;
    int price;
    int orderType;
    int id;
    struct AccountName_struct * account;
    char * ts;
    struct FillNode_struct * firstfillnode;
    int totalFilled;
    int open;
} ORDER;

typedef struct OrderNode_struct {
    struct Order_struct * order;
    struct OrderNode_struct * prev;
    struct OrderNode_struct * next;
} ORDERNODE;

typedef struct Level_struct {
    struct Level_struct * prev;
    struct Level_struct * next;
    int price;
    struct OrderNode_struct * firstordernode;
} LEVEL;

typedef struct OrderPtrAndError_struct {
    struct Order_struct * order;
    int error;
} ORDER_AND_ERROR;


// Globals......


char Venue[MAXTOKENSIZE];
char Symbol[MAXTOKENSIZE];

LEVEL * FirstBidLevel = NULL;
LEVEL * FirstAskLevel = NULL;

char * LastTradeTime = NULL;

int LastPrice = -1;
int LastSize = -1;

ORDER ** AllOrders = NULL;
int CurrentOrderArrayLen = 0;
int HighestKnownOrder = -1;

ACCOUNT ** AllAccounts = NULL;
int CurrentAccountArrayLen = 0;
int HighestKnownAccount = -1;



void end_message(void)
{
    printf("\nEND\n");
    fflush(stdout);
    return;
}


void check_ram_or_die(void * ptr)
{
    if (ptr == NULL)
    {
        printf("{\"ok\": false, \"error\": \"Out of memory! Quitting\"}");
        end_message();
        assert(ptr);
    }
    return;
}


LEVEL * init_level(int price, ORDERNODE * ordernode, LEVEL * prev, LEVEL * next)
{
    LEVEL * ret;
    
    ret = malloc(sizeof(LEVEL));
    check_ram_or_die(ret);
    
    ret->price = price;
    ret->firstordernode = ordernode;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


FILL * init_fill(int price, int qty, char * ts)
{
    FILL * ret;
    
    ret = malloc(sizeof(FILL));
    check_ram_or_die(ret);
    
    ret->price = price;
    ret->qty = qty;
    ret->ts = ts;
    
    return ret;
}


FILLNODE * init_fillnode(FILL * fill, FILLNODE * prev, FILLNODE * next)
{
    FILLNODE * ret;
    
    ret = malloc(sizeof(FILLNODE));
    check_ram_or_die(ret);
    
    ret->fill = fill;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


ORDERNODE * init_ordernode(ORDER * order, ORDERNODE * prev, ORDERNODE * next)
{
    ORDERNODE * ret;
    
    ret = malloc(sizeof(ORDERNODE));
    check_ram_or_die(ret);
    
    ret->order = order;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


int next_id(int no_iterate_flag)
{
    static int id = 0;
    
    if (id == MAXORDERS)         // Stop iterating
    {
        return MAXORDERS;
    } else {
        if (no_iterate_flag)
        {
            return id;
        } else {
            return id++;
        }
    }
}


char * mod_strncpy(char * s1, const char * s2, int max)
{
    char * ret;

    ret = strncpy(s1, s2, max);
    s1[max - 1] = '\0';

    return ret;
}


char * new_timestamp(void)
{
    char * timestamp;
    
    timestamp = malloc(32);
    check_ram_or_die(timestamp);
    
    mod_strncpy(timestamp, "FIXME", 32);
    
    return timestamp;
}


ORDER * init_order(ACCOUNT * account, int qty, int price, int direction, int orderType, int id)
{
    ORDER * ret;
    
    ret = malloc(sizeof(ORDER));
    check_ram_or_die(ret);
    
    ret->direction = direction;
    ret->originalQty = qty;
    ret->qty = qty;
    ret->price = price;
    ret->orderType = orderType;
    ret->id = id;
    ret->account = account;
    ret->ts = new_timestamp();
    ret->firstfillnode = NULL;
    ret->totalFilled = 0;
    ret->open = 1;

    // Now deal with the global order storage...
    
    if (id >= CurrentOrderArrayLen)
    {
        AllOrders = realloc(AllOrders, (CurrentOrderArrayLen + 8192) * sizeof(ORDER *));
        check_ram_or_die(AllOrders);
        CurrentOrderArrayLen += 8192;
    }
    AllOrders[id] = ret;
    HighestKnownOrder = id;
    
    return ret;
}


ORDER_AND_ERROR * init_o_and_e()
{
    ORDER_AND_ERROR * ret;
    
    ret = malloc(sizeof(ORDER_AND_ERROR));
    check_ram_or_die(ret);
    
    ret->order = NULL;
    ret->error = 0;
    
    return ret;
}


void cross(ORDER * standing, ORDER * incoming)
{
    int quantity;
    int price;
    char * ts;
    FILLNODE * currentfillnode;
    FILL * fill;
    
    ts = new_timestamp();
    
    if (standing->qty < incoming->qty)
    {
        quantity = standing->qty;
    } else {
        quantity = incoming->qty;
    }
    
    standing->qty -= quantity;
    standing->totalFilled += quantity;
    incoming->qty -= quantity;
    incoming->totalFilled += quantity;
    
    price = standing->price;

    LastTradeTime = ts;
    LastPrice = price;
    LastSize = quantity;
    
    fill = init_fill(price, quantity, ts);

    // Figure out where to put the fill...
    
    if (standing->firstfillnode == NULL)
    {
        standing->firstfillnode = init_fillnode(fill, NULL, NULL);
    } else {
        currentfillnode = standing->firstfillnode;
        while (currentfillnode->next != NULL)
        {
            currentfillnode = currentfillnode->next;
        }
        currentfillnode->next = init_fillnode(fill, currentfillnode, NULL);
    }
    
    // Again for other order...
    
    if (incoming->firstfillnode == NULL)
    {
        incoming->firstfillnode = init_fillnode(fill, NULL, NULL);
    } else {
        currentfillnode = incoming->firstfillnode;
        while (currentfillnode->next != NULL)
        {
            currentfillnode = currentfillnode->next;
        }
        currentfillnode->next = init_fillnode(fill, currentfillnode, NULL);
    }
    
    if (standing->qty == 0) standing->open = 0;
    if (incoming->qty == 0) incoming->open = 0;
    
    return;
}


void run_order(ORDER * order)
{
    LEVEL * current_level;
    ORDERNODE * current_node;
    
    if (order->direction == SELL)
    {
        current_level = FirstBidLevel;
        while (current_level != NULL)
        {
            if (current_level->price < order->price && order->orderType != MARKET) return;
            current_node = current_level->firstordernode;
            while (current_node != NULL)
            {
                cross(current_node->order, order);
                if (order->open == 0) return;
                current_node = current_node->next;
            }
            current_level = current_level->next;
        }
    } else {
        current_level = FirstAskLevel;
        while (current_level != NULL)
        {
            if (current_level->price > order->price && order->orderType != MARKET) return;
            current_node = current_level->firstordernode;
            while (current_node != NULL)
            {
                cross(current_node->order, order);
                if (order->open == 0) return;
                current_node = current_node->next;
            }
            current_level = current_level->next;
        }
    }
    
    return;
}


void cleanup_closed_bids(void)
{
    LEVEL * current_level;
    LEVEL * old_level;
    ORDERNODE * current_node;
    ORDERNODE * old_node;
    
    if (FirstBidLevel == NULL) return;
    
    current_level = FirstBidLevel;
    current_node = current_level->firstordernode;
    assert(current_node != NULL);
    
    while (1)
    {
        if (current_node->order->open)
        {
            current_level->firstordernode = current_node;
            current_node->prev = NULL;
            FirstBidLevel = current_level;
            FirstBidLevel->prev = NULL;
            return;
        }
        
        if (current_node->next != NULL)
        {
            old_node = current_node;
            current_node = current_node->next;
            free(old_node);
        } else {
            old_node = current_node;
            old_level = current_level;
            current_level = current_level->next;
            free(old_node);
            free(old_level);
            if (current_level != NULL)
            {
                current_node = current_level->firstordernode;
                assert(current_node != NULL);
            } else {
                FirstBidLevel = NULL;
                return;
            }
        }
    }

    return;
}


void cleanup_closed_asks(void)      // This and the above could be consolidated into a single function...
{
    LEVEL * current_level;
    LEVEL * old_level;
    ORDERNODE * current_node;
    ORDERNODE * old_node;
    
    if (FirstAskLevel == NULL) return;
    
    current_level = FirstAskLevel;
    current_node = current_level->firstordernode;
    assert(current_node != NULL);
    
    while (1)
    {
        if (current_node->order->open)
        {
            current_level->firstordernode = current_node;
            current_node->prev = NULL;
            FirstAskLevel = current_level;
            FirstAskLevel->prev = NULL;
            return;
        }
        
        if (current_node->next != NULL)
        {
            old_node = current_node;
            current_node = current_node->next;
            free(old_node);
        } else {
            old_node = current_node;
            old_level = current_level;
            current_level = current_level->next;
            free(old_node);
            free(old_level);
            if (current_level != NULL)
            {
                current_node = current_level->firstordernode;
                assert(current_node != NULL);
            } else {
                FirstAskLevel = NULL;
                return;
            }
        }
    }

    return;
}

    
void insert_ask(ORDER * order)
{
    ORDERNODE * ordernode;
    ORDERNODE * current_node;
    LEVEL * prev_level = NULL;
    LEVEL * level;
    LEVEL * newlevel;
    
    ordernode = init_ordernode(order, NULL, NULL);      // Fix ->prev later
    
    if (FirstAskLevel == NULL)
    {
        FirstAskLevel = init_level(order->price, ordernode, NULL, NULL);
        return;
    } else {
        level = FirstAskLevel;
    }
    
    while (1)
    {
        if (order->price < level->price)
        {
            // Create new level...
            
            newlevel = init_level(order->price, ordernode, prev_level, level);
            level->prev = newlevel;
            if (prev_level)
            {
                prev_level->next = newlevel;
            } else {
                FirstAskLevel = newlevel;
            }
            return;
            
        } else if (order->price == level->price) {
            
            break;
            
        } else {
            
            // Iterate...
            
            prev_level = level;
            if (level->next != NULL)
            {
                level = level->next;
            } else {
                level->next = init_level(order->price, ordernode, prev_level, NULL);
                return;
            }
        }
    }
    
    // So we should be on the right level now
    
    assert(level->price == order->price);
    
    current_node = level->firstordernode;
    assert(current_node != NULL);
    
    while (current_node->next != NULL)
    {
        current_node = current_node->next;
    }
    
    current_node->next = ordernode;
    ordernode->prev = current_node;
    
    return;
}


void insert_bid(ORDER * order)
{
    ORDERNODE * ordernode;
    ORDERNODE * current_node;
    LEVEL * prev_level = NULL;
    LEVEL * level;
    LEVEL * newlevel;
    
    ordernode = init_ordernode(order, NULL, NULL);      // Fix ->prev later
    
    if (FirstBidLevel == NULL)
    {
        FirstBidLevel = init_level(order->price, ordernode, NULL, NULL);
        return;
    } else {
        level = FirstBidLevel;
    }
    
    while (1)
    {
        if (order->price > level->price)
        {
            // Create new level...
            
            newlevel = init_level(order->price, ordernode, prev_level, level);
            level->prev = newlevel;
            if (prev_level)
            {
                prev_level->next = newlevel;
            } else {
                FirstBidLevel = newlevel;
            }
            return;
            
        } else if (order->price == level->price) {
            
            break;
            
        } else {
            
            // Iterate...
            
            prev_level = level;
            if (level->next != NULL)
            {
                level = level->next;
            } else {
                level->next = init_level(order->price, ordernode, prev_level, NULL);
                return;
            }
        }
    }
    
    // So we should be on the right level now
    
    assert(level->price == order->price);
    
    current_node = level->firstordernode;
    assert(current_node != NULL);
    
    while (current_node->next != NULL)
    {
        current_node = current_node->next;
    }
    
    current_node->next = ordernode;
    ordernode->prev = current_node;
    
    return;
}


ACCOUNT * init_account(char * name)
{
    ACCOUNT * ret;
    
    ret = malloc(sizeof(ACCOUNT));
    check_ram_or_die(ret);
    
    mod_strncpy(ret->name, name, MAXTOKENSIZE);
    
    return ret;
}


ORDER_AND_ERROR * parse_order(char * account_name, int account_int, int qty, int price, int direction, int orderType)
{
    // Note: account_name will be in the stack of the calling function, not in the heap
    
    ORDER * order;
    ORDER_AND_ERROR * o_and_e;
    ACCOUNT * account;
    
    int id;
    
    o_and_e = init_o_and_e();
    
    // Check for too high an order ID...
    
    id = next_id(0);
    if (id >= MAXORDERS)
    {
        o_and_e->error = TOO_MANY_ORDERS;
        return o_and_e;
    }
    
    // Check if account_int is unknown, in which case create an account struct to hold its name.
    // Also extend the array of all known accounts if needed.
    
    if (account_int > HighestKnownAccount)
    {
        if (account_int >= CurrentAccountArrayLen)
        {
            AllAccounts = realloc(AllAccounts, (CurrentAccountArrayLen + 1024) * sizeof(ACCOUNT *));
            check_ram_or_die(AllAccounts);
            CurrentAccountArrayLen += 1024;
        }
        AllAccounts[account_int] = init_account(account_name);
    }
    
    // Create order struct...
    
    order = init_order(AllAccounts[account_int], qty, price, direction, orderType, id);
    
    if (order->orderType != FOK)
    {
        run_order(order);
    } else {
        // Some stuff to deal with FOK orders
    }
    
    if (order->direction == SELL)
    {
        cleanup_closed_bids();
    } else {
        cleanup_closed_asks();
    }
    
    if (order->orderType == MARKET)
    {
        order->price = 0;       // This is the official behaviour
    }
    
    if (order->open)
    {
        if (order->orderType == LIMIT)
        {
            if (order->direction == SELL)
            {
                insert_ask(order);
            } else {
                insert_bid(order);
            }
        } else {
            order->open = 0;
            order->qty = 0;
        }
    }
    
    o_and_e->order = order;
    
    return o_and_e;
}


void print_fills(ORDER * order)
{
    FILLNODE * fillnode;
    
    if (order->firstfillnode == NULL)   // Can do without this block but it's uglier
    {
        printf("\"fills\": []");
        return;
    }
    
    printf("\"fills\": [\n");
    
    fillnode = order->firstfillnode;
    
    while (fillnode != NULL)
    {
        if (fillnode != order->firstfillnode) printf(",\n");
        printf("{\"price\": %d, \"qty\": %d, \"ts\": \"%s\"}", fillnode->fill->price, fillnode->fill->qty, fillnode->fill->ts);
        fillnode = fillnode->next;
    }
    
    printf("\n]");
    return;
}


void print_order(ORDER * order)
{
    char orderType_to_print[MAXTOKENSIZE];
    
    if (order->orderType == LIMIT)
    {
        mod_strncpy(orderType_to_print, "limit", MAXTOKENSIZE);
    } else if (order->orderType == MARKET) {
        mod_strncpy(orderType_to_print, "market", MAXTOKENSIZE);
    } else if (order->orderType == IOC) {
        mod_strncpy(orderType_to_print, "immediate-or-cancel", MAXTOKENSIZE);
    } else if (order->orderType == FOK) {
        mod_strncpy(orderType_to_print, "fill-or-kill", MAXTOKENSIZE);
    } else {
        mod_strncpy(orderType_to_print, "unknown", MAXTOKENSIZE);
    }
    
    printf("{\"ok\": true, \"venue\": \"%s\", \"symbol\": \"%s\", \"direction\": \"%s\", \"originalQty\": %d, \"qty\": %d, \"price\": %d, \"orderType\": \"%s\", \"id\": %d, \"account\": \"%s\", \"ts\": \"%s\", \"totalFilled\": %d, \"open\": %s,\n",
            Venue, Symbol, order->direction == BUY ? "buy" : "sell", order->originalQty, order->qty, order->price, orderType_to_print,
            order->id, order->account->name, order->ts, order->totalFilled, order->open ? "true" : "false");
    
    print_fills(order);
    printf("}");
    
    return;
}


LEVEL * find_level(int price, int dir)      // Return ptr to level, or return NULL if not present
{
    LEVEL * level = NULL;
            
    if (dir == BUY)
    {
        level = FirstBidLevel;
        while (level != NULL)
        {
            if (level->price > price)
            {
                level = level->next;
            } else if (level->price == price) {
                break;
            } else {
                level = NULL;
                break;
            }
        }
    } else {
        level = FirstAskLevel;
        while (level != NULL)
        {
            if (level->price < price)
            {
                level = level->next;
            } else if (level->price == price) {
                break;
            } else {
                level = NULL;
                break;
            }
        }
    }
    
    return level;
}


ORDERNODE * find_ordernode(LEVEL * level, int id)
{
    ORDERNODE * ordernode;
    
    if (level)
    {
        ordernode = level->firstordernode;
        assert(ordernode != NULL);
        
        while (ordernode != NULL)
        {
            if (ordernode->order->id == id)
            {
                return ordernode;
            }
            ordernode = ordernode->next;
        }
    }
    
    return NULL;
}


void cleanup_after_cancel(ORDERNODE * ordernode, LEVEL * level)       // Free the ordernode, maybe free the level, fix all links
{
    int dir;
    
    assert(ordernode && level);
    
    dir = ordernode->order->direction;                  // Needed later
    
    
    if (ordernode->prev)
    {
        ordernode->prev->next = ordernode->next;
    } else {
        level->firstordernode = ordernode->next;        // Can set level->firstordernode to NULL, in which case
    }                                                   // the level is now empty and must be destroyed in a bit
    
    if (ordernode->next)
    {
        ordernode->next->prev = ordernode->prev;
    }
    
    free(ordernode);
    
    if (level->firstordernode == NULL)
    {
        if (level->prev)
        {
            level->prev->next = level->next;
        } else {
            if (dir == BUY)
            {
                FirstBidLevel = level->next;
            } else {
                FirstAskLevel = level->next;
            }
        }
        
        if (level->next)
        {
            level->next->prev = level->prev;
        }
        
        free(level);
    }
    
    return;
}


int main(int argc, char ** argv)
{
    char * eofcheck;
    char * tmp;
    char input[MAXINPUT];
    char tokens[MAXTOKENS][MAXTOKENSIZE];
    int token_count;
    
    // The following are all general-purpose vars to be used wherever needed, they don't store long-term info
    
    int n;
    int flag;
    ORDER * order;
    ORDER_AND_ERROR * o_and_e;
    LEVEL * level;
    ORDERNODE * ordernode;
    int price;
    int id;
    int dir;
    int orderType;
    
    assert(argc == 3);
    
    mod_strncpy(Venue, argv[1], MAXTOKENSIZE);
    mod_strncpy(Symbol, argv[2], MAXTOKENSIZE);
    
    while (1)
    {
        eofcheck = fgets(input, MAXINPUT, stdin);
        
        if (eofcheck == NULL)           // i.e. we HAVE reached EOF
        {
            printf("{\"ok\": false, \"error\": \"Unexpected EOF on stdin. Quitting.\"}");
            end_message();
            return 1;
        }
        
        token_count = 0;
        tmp = strtok(input, " \t\n\r");
        for (n = 0; n < MAXTOKENS; n++)
        {
            tokens[n][0] = '\0';        // Clear the token in case there isn't one in this slot
            if (tmp != NULL)
            {
                mod_strncpy(tokens[n], tmp, MAXTOKENSIZE);
                token_count += 1;
                tmp = strtok(NULL, " \t\n\r");
            }
        }
        
        // ---------------------------------------------- ORDER ----------------------------------------------------------
        
        if (strcmp("ORDER", tokens[0]) == 0)
        {
            o_and_e = parse_order(tokens[1], atoi(tokens[2]), atoi(tokens[3]), atoi(tokens[4]), atoi(tokens[5]), atoi(tokens[6]));
            //                    account    account_int      qty              price            direction        orderType
            
            if (o_and_e->error)
            {
                printf("{\"ok\": false, \"error\": \"Backend error %d\"}", o_and_e->error);
            } else {
                print_order(o_and_e->order);
            }
            
            free(o_and_e);
            end_message();
            continue;
        }
        
        // -------------------------------------- ORDER BOOK -------------------------------------------------------------
        
        if (strcmp("ORDERBOOK", tokens[0]) == 0)
        {
            printf("{\"ok\": true, \"venue\": \"%s\", \"symbol\": \"%s\", \"ts\": \"FIXME\",\n", Venue, Symbol);
            
            printf("\"asks\": [");
            level = FirstAskLevel;
            flag = 0;
            while (level != NULL)
            {
                ordernode = level->firstordernode;
                while (ordernode != NULL)
                {
                    if (flag) printf(", \n");
                    printf("{\"price\": %d, \"qty\": %d, \"isBuy\": false}", ordernode->order->price, ordernode->order->qty);
                    flag = 1;
                    ordernode = ordernode->next;
                }
                level = level->next;
            }
            printf("],\n");
            
            printf("\"bids\": [");
            level = FirstBidLevel;
            flag = 0;
            while (level != NULL)
            {
                ordernode = level->firstordernode;
                while (ordernode != NULL)
                {
                    if (flag) printf(", \n");
                    printf("{\"price\": %d, \"qty\": %d, \"isBuy\": true}", ordernode->order->price, ordernode->order->qty);
                    flag = 1;
                    ordernode = ordernode->next;
                }
                level = level->next;
            }
            printf("]}");
            
            end_message();
            continue;
        }
        
        // -------------------------------------- ORDER STATUS -----------------------------------------------------------
        
        if (strcmp("STATUS", tokens[0]) == 0)
        {
            id = atoi(tokens[1]);
            if (id < 0 || id > HighestKnownOrder)
            {
                printf("{\"ok\": false, \"error\": \"No such ID\"}");
                end_message();
                continue;
            }
            
            print_order(AllOrders[id]);
            
            end_message();
            continue;
        }
        
        // ---------------------------------------- CANCEL ---------------------------------------------------------------
        
        if (strcmp("CANCEL", tokens[0]) == 0)
        {
            id = atoi(tokens[1]);
            if (id < 0 || id > HighestKnownOrder)
            {
                printf("{\"ok\": false, \"error\": \"No such ID\"}");
                end_message();
                continue;
            }
            
            orderType = AllOrders[id]->orderType;
            
            if (orderType != LIMIT)                     // Everything else is auto-cancelled after running
            {
                print_order(AllOrders[id]);
                end_message();
                continue;
            }

            price = AllOrders[id]->price;
            dir = AllOrders[id]->direction;
            
            // Find the level then the ordernode, if possible...
            
            level = find_level(price, dir);
            ordernode = find_ordernode(level, id);      // This is safe even if level == NULL
            
            // Now close the order and do the linked-list fiddling...
            
            if (ordernode)
            {
                ordernode->order->open = 0;
                ordernode->order->qty = 0;
                
                cleanup_after_cancel(ordernode, level);   // Frees the node and even the level if needed; fixes links
            }
            
            print_order(AllOrders[id]);
            end_message();
            continue;
        }
        
        // ------------------------------ FRONT-END REQUEST FOR ACCOUNT OF ORDER -----------------------------------------
        
        if (strcmp("__ACC_FROM_ID__", tokens[0]) == 0)
        {
            id = atoi(tokens[1]);
            if (id < 0 || id > HighestKnownOrder)
            {
                printf("ERROR None");
                end_message();
                continue;
            }
            
            printf("OK %s", AllOrders[id]->account->name);
            end_message();
            continue;
        }
        
        // ---------------------------------------------------------------------------------------------------------------
        
        printf("{\"ok\": false, \"error\": \"Did not comprehend\"}");
        end_message();
        continue;
    }
    
    return 0;
}
