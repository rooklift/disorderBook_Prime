/*  Crazy attempt to write the disorderBook backend in C.
    The data layout stolen from DanielVF.
   
    We store all data in memory so that the user can retrieve it later.
    As such, there are very few free() calls.


    PROTOCOL:
    
    We don't handle user input directly. The frontend is responsible for
    sending us commands as single lines. Only the ORDER command is tricky:
    
    ORDER <account> <account_id> <qty> <price> <dir:1|2> <orderType:1|2|3|4>
    
    e.g.
    
    ORDER CES134127  5            100   5000    1         3
    
    The frontend must give each account a unique, low, non-negative integer as
    an id (RAM is allocated based on these, so keep them as low as possible).
    
    Numbers for direction and orderType are defined below.
    
    Other commands:
    
    QUOTE
    ORDERBOOK
    CANCEL <id>
    STATUS <id>
    __SCORE__
    __DEBUG_MEMORY__
    __ACC_FROM_ID__ <id>
    
    This last is not a direct response to a user query, but can be used by the
    frontend for authentication purposes (i.e. is the user entitled to cancel
    this order?)
    
    
    */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define BUY 1       // Don't change these now, they are also used in the frontend
#define SELL 2

#define LIMIT 1     // Don't change these now, they are also used in the frontend
#define MARKET 2
#define FOK 3
#define IOC 4

#define MAXSTRING 2048
#define SMALLSTRING 64
#define MAXTOKENS 64                // Well-behaved frontend will never send this many

#define MAXORDERS 2000000000        // Not going all the way to MAX_INT, because various numbers might go above this
                  
#define TOO_MANY_ORDERS 1
#define SILLY_VALUE 2


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

typedef struct Account_struct {
    char name[SMALLSTRING];
    struct Order_struct ** orders;
    int arraylen;
    int count;
    int posmin;
    int posmax;
    int shares;
    int cents;
} ACCOUNT;

typedef struct Order_struct {
    int direction;
    int originalQty;
    int qty;
    int price;
    int orderType;
    int id;
    struct Account_struct * account;
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



typedef struct DebugInfo_struct {
    int inits_of_level;
    int inits_of_fill;
    int inits_of_fillnode;
    int inits_of_order;
    int inits_of_ordernode;
    int inits_of_account;
    
    int reallocs_of_global_order_list;
    int reallocs_of_global_account_list;
    int reallocs_of_account_order_list;
} DEBUG_INFO;
    


// ---------------------------------- GLOBALS -----------------------------------------------


char Venue[SMALLSTRING];
char Symbol[SMALLSTRING];

LEVEL * FirstBidLevel = NULL;
LEVEL * FirstAskLevel = NULL;

char * LastTradeTime = NULL;
int LastPrice = -1;             // Don't change this now, is checked by score function
int LastSize = -1;

ORDER ** AllOrders = NULL;
int CurrentOrderArrayLen = 0;
int HighestKnownOrder = -1;

ACCOUNT ** AllAccounts = NULL;
int CurrentAccountArrayLen = 0;

DEBUG_INFO DebugInfo = {0};     // Think global is auto-zeroed anyway, but whatever


// ------------------------------------------------------------------------------------------


void end_message(void)
{
    printf("\nEND\n");
    fflush(stdout);
    return;
}


void check_ptr_or_quit(void * ptr)
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
    
    DebugInfo.inits_of_level++;
    
    ret = malloc(sizeof(LEVEL));
    check_ptr_or_quit(ret);
    
    ret->price = price;
    ret->firstordernode = ordernode;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


FILL * init_fill(int price, int qty, char * ts)
{
    FILL * ret;
    
    DebugInfo.inits_of_fill++;
    
    ret = malloc(sizeof(FILL));
    check_ptr_or_quit(ret);
    
    ret->price = price;
    ret->qty = qty;
    ret->ts = ts;
    
    return ret;
}


FILLNODE * init_fillnode(FILL * fill, FILLNODE * prev, FILLNODE * next)
{
    FILLNODE * ret;
    
    DebugInfo.inits_of_fillnode++;
    
    ret = malloc(sizeof(FILLNODE));
    check_ptr_or_quit(ret);
    
    ret->fill = fill;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


ORDERNODE * init_ordernode(ORDER * order, ORDERNODE * prev, ORDERNODE * next)
{
    ORDERNODE * ret;
    
    DebugInfo.inits_of_ordernode++;
    
    ret = malloc(sizeof(ORDERNODE));
    check_ptr_or_quit(ret);
    
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


// strlcpy stolen straight from OpenBSD
// Copyright (c) 1998 Todd C. Miller
//
// BSD License (as is disorderCook)
// I renamed the function in case the compiler already knows about strlcpy

size_t bk_strlcpy(char * dst, const char * src, size_t siz)
{
    char * d = dst;
    const char * s = src;
    size_t n = siz;

    // Copy as many bytes as will fit
    if (n != 0 && --n != 0) {
        do {
            if ((*d++ = *s++) == 0)
                break;
        } while (--n != 0);
    }

    // Not enough room in dst, add NUL and traverse rest of src
    if (n == 0) {
        if (siz != 0)
            *d = '\0';      // NUL-terminate dst
        while (*s++)
            ;
    }

    return(s - src - 1);    // count does not include NUL
}


char * new_timestamp(void)
{
    char * timestamp;
    time_t t;
    struct tm * ti;
    
    timestamp = malloc(SMALLSTRING);
    check_ptr_or_quit(timestamp);
    
    t = time(NULL);
    
    if (t != (time_t) -1)
    {
        ti = gmtime(&t);
    } else {
        ti = NULL;
    }
    
    if (ti)
    {
        snprintf(timestamp, SMALLSTRING, "%d-%02d-%02dT%02d:%02d:%02d.0000Z", ti->tm_year + 1900, ti->tm_mon + 1, ti->tm_mday, ti->tm_hour, ti->tm_min, ti->tm_sec);
    } else {
        snprintf(timestamp, SMALLSTRING, "Unknown");
    }
    
    return timestamp;
}


ORDER * init_order(ACCOUNT * account, int qty, int price, int direction, int orderType, int id)
{
    ORDER * ret;
    
    DebugInfo.inits_of_order++;
    
    ret = malloc(sizeof(ORDER));
    check_ptr_or_quit(ret);
    
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
    
    while (id >= CurrentOrderArrayLen)
    {
        AllOrders = realloc(AllOrders, (CurrentOrderArrayLen + 8192) * sizeof(ORDER *));
        check_ptr_or_quit(AllOrders);
        CurrentOrderArrayLen += 8192;
        
        DebugInfo.reallocs_of_global_order_list++;
    }
    AllOrders[id] = ret;
    HighestKnownOrder = id;
    
    return ret;
}


ORDER_AND_ERROR * init_o_and_e()
{
    ORDER_AND_ERROR * ret;
    
    ret = malloc(sizeof(ORDER_AND_ERROR));
    check_ptr_or_quit(ret);
    
    ret->order = NULL;
    ret->error = 0;
    
    return ret;
}


void update_account(ACCOUNT * account, int quantity, int price, int direction)
{
    int64_t tmp64;
    
    assert(account);

    // Update shares...
    
    if (direction == BUY)
    {
        if (account->shares > 0)
        {
            if ((2147483647 - account->shares) - quantity < 0)
            {
                account->shares = 2147483647;
            } else {
                account->shares += quantity;
            }
        } else {
            account->shares += quantity;
        }
    } else {
        if (account->shares < 0)
        {
            if ((-2147483647 - account->shares) + quantity > 0)         // Don't write -2147483648
            {
                account->shares = -2147483647;
            } else {
                account->shares -= quantity;
            }
        } else {
            account->shares -= quantity;
        }
    }
    
    // Update cents...
    
    tmp64 = account->cents;
    
    if (direction == BUY)
    {
        tmp64 -= (int64_t) price * (int64_t) quantity;
        if (tmp64 < -2147483647)
        {
            account->cents = -2147483647;
        } else {
            account->cents = (int) tmp64;
        }
    } else {
        tmp64 += (int64_t) price * (int64_t) quantity;
        if (tmp64 > 2147483647)
        {
            account->cents = 2147483647;
        } else {
            account->cents = (int) tmp64;
        }
    }
    
    if (account->shares < account->posmin) account->posmin = account->shares;
    if (account->shares > account->posmax) account->posmax = account->shares;
    
    return;
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
    
    // Fix the positions of the 2 accounts...
    
    if (standing->direction == BUY)
    {
        update_account(standing->account, quantity, price, BUY);
        update_account(incoming->account, quantity, price, SELL);
    } else {
        update_account(standing->account, quantity, price, SELL);
        update_account(incoming->account, quantity, price, BUY);
    }
    
    return;
}


void run_order(ORDER * order)
{
    LEVEL * current_level;
    ORDERNODE * current_node;
    
    if (order->direction == SELL)
    {
        for (current_level = FirstBidLevel; current_level != NULL; current_level = current_level->next)
        {
            if (current_level->price < order->price && order->orderType != MARKET) return;
            
            for (current_node = current_level->firstordernode; current_node != NULL; current_node = current_node->next)
            {
                cross(current_node->order, order);
                if (order->open == 0) return;
            }
        }
    } else {
        for (current_level = FirstAskLevel; current_level != NULL; current_level = current_level->next)
        {
            if (current_level->price > order->price && order->orderType != MARKET) return;

            for (current_node = current_level->firstordernode; current_node != NULL; current_node = current_node->next)
            {
                cross(current_node->order, order);
                if (order->open == 0) return;
            }
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


int fok_can_buy(int qty, int price)
{
    // Must use subtraction only. Adding could overflow.
    
    LEVEL * level;
    ORDERNODE * ordernode;
    
    for (level = FirstAskLevel; level != NULL && level->price <= price; level = level->next)
    {
        for (ordernode = level->firstordernode; ordernode != NULL; ordernode = ordernode->next)
        {
            qty -= ordernode->order->qty;
            if (qty <= 0) return 1;
        }
        
    }
    
    return 0;
}


int fok_can_sell(int qty, int price)
{
    // Must use subtraction only. Adding could overflow.
    
    LEVEL * level;
    ORDERNODE * ordernode;
    
    for (level = FirstBidLevel; level != NULL && level->price >= price; level = level->next)
    {
        for (ordernode = level->firstordernode; ordernode != NULL; ordernode = ordernode->next)
        {
            qty -= ordernode->order->qty;
            if (qty <= 0) return 1;
        }
        
    }
    
    return 0;
}


ACCOUNT * init_account(char * name)
{
    ACCOUNT * ret;
    
    DebugInfo.inits_of_account++;
    
    ret = malloc(sizeof(ACCOUNT));
    check_ptr_or_quit(ret);
    
    bk_strlcpy(ret->name, name, SMALLSTRING);
    
    ret->orders = NULL;
    ret->arraylen = 0;
    ret->count = 0;
    
    ret->posmin = 0;
    ret->posmax = 0;
    ret->shares = 0;
    ret->cents = 0;
    
    return ret;
}


ACCOUNT * account_lookup_or_create (char * account_name, int account_int)
{
    int n;
    
    // If account_id is too high, we will need more storage...
    
    while (account_int >= CurrentAccountArrayLen)
    {
        AllAccounts = realloc(AllAccounts, (CurrentAccountArrayLen + 64) * sizeof(ACCOUNT *));
        check_ptr_or_quit(AllAccounts);
        CurrentAccountArrayLen += 64;
        
        // We must NULLify our new account pointers because there can be holes in the known
        // account IDs: e.g. known IDs are 0,1,2,3,7. So, if we're asked to lookup ID 5, we
        // need a way to know it doesn't exist...
        
        for (n = CurrentAccountArrayLen - 64; n < CurrentAccountArrayLen; n++)
        {
            AllAccounts[n] = NULL;
        }
        
        DebugInfo.reallocs_of_global_account_list++;
    }

    // If the account corresponsing to the account_id is NULL, create it...
    
    if (AllAccounts[account_int] == NULL)
    {
        AllAccounts[account_int] = init_account(account_name);
    }
    
    // Done...
    
    return AllAccounts[account_int];
}


void add_order_to_account(ORDER * order, ACCOUNT * accountobject)
{
    if (accountobject->count == accountobject->arraylen)
    {
        accountobject->orders = realloc(accountobject->orders, (accountobject->arraylen + 256) * sizeof(ORDER *));
        check_ptr_or_quit(accountobject->orders);
        accountobject->arraylen += 256;
        
        DebugInfo.reallocs_of_account_order_list++;
    }
    accountobject->orders[accountobject->count] = order;
    accountobject->count += 1;
    
    return;
}


ORDER_AND_ERROR * parse_order(char * account_name, int account_int, int qty, int price, int direction, int orderType)
{
    // Note: account_name will be in the stack of the calling function, not in the heap
    
    ORDER * order;
    ORDER_AND_ERROR * o_and_e;
    int id;
    ACCOUNT * accountobject;
    
    // The o_and_e structure lets us send either an order or an error to the caller...
    
    o_and_e = init_o_and_e();
    
    // Check for too high an order ID...
    
    id = next_id(0);
    if (id >= MAXORDERS)
    {
        o_and_e->error = TOO_MANY_ORDERS;
        return o_and_e;
    }
    
    // Check for silly values...
    
    if (price < 0 || qty < 1 || (direction != SELL && direction != BUY))
    {
        o_and_e->error = SILLY_VALUE;
        return o_and_e;
    }
    
    // The following call gets the account object. If not already extant, it is created.
    // If more memory is needed to store accounts up to this account_id, that happens...
    
    accountobject = account_lookup_or_create(account_name, account_int);
    
    // Create order struct, and store a pointer to it in the account...
    
    order = init_order(accountobject, qty, price, direction, orderType, id);
    add_order_to_account(order, accountobject);
    
    // Run the order, with checks for FOK if needed...
    
    if (order->orderType != FOK)
    {
        run_order(order);
    } else {
        if (order->direction == BUY)
        {
            if (fok_can_buy(order->qty, order->price))
            {
                run_order(order);
            }
        } else {
            if (fok_can_sell(order->qty, order->price))
            {
                run_order(order);
            }
        }
    }
    
    // Iterate through the Bids or Asks as appropriate, removing them from the book if they are now closed...
    
    if (order->direction == SELL)
    {
        cleanup_closed_bids();
    } else {
        cleanup_closed_asks();
    }
    
    // Market orders get set to price == 0 in official for storage / reporting
    // (the timing doesn't matter, this could be done before running the order)
  
    if (order->orderType == MARKET) order->price = 0;

    // Place open limit orders on the book. Mark other order types as closed...
    
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
    char orderType_to_print[SMALLSTRING];
    
    if (order->orderType == LIMIT)
    {
        bk_strlcpy(orderType_to_print, "limit", SMALLSTRING);
    } else if (order->orderType == MARKET) {
        bk_strlcpy(orderType_to_print, "market", SMALLSTRING);
    } else if (order->orderType == IOC) {
        bk_strlcpy(orderType_to_print, "immediate-or-cancel", SMALLSTRING);
    } else if (order->orderType == FOK) {
        bk_strlcpy(orderType_to_print, "fill-or-kill", SMALLSTRING);
    } else {
        bk_strlcpy(orderType_to_print, "unknown", SMALLSTRING);
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


int get_size_from_level(LEVEL * level)
{
    ORDERNODE * ordernode;
    int ret;
    
    if (level == NULL)
    {
        return 0;
    }
    
    ret = 0;
    
    for (ordernode = level->firstordernode; ordernode != NULL; ordernode = ordernode->next)
    {
        if ((2147483647 - ret) - ordernode->order->qty < 0)
        {
            return 2147483647;
        } else {
            ret += ordernode->order->qty;
        }
    }
    return ret;
}


int get_depth(LEVEL * level)        // Returns size of this level and all worse levels (if level exists)
{
    int onesize;
    int ret;
    
    ret = 0;

    for ( ; level != NULL; level = level->next)
    {
        onesize = get_size_from_level(level);
        
        if ((2147483647 - ret) - onesize < 0)
        {
            return 2147483647;
        } else {
            ret += onesize;
        }
    }
    return ret;
}


int main(int argc, char ** argv)
{
    char * eofcheck;
    char * tmp;
    char input[MAXSTRING];
    char tokens[MAXTOKENS][SMALLSTRING];
    int token_count;
    
    // The following are all general-purpose vars to be used wherever needed, they don't store long-term info
    
    int n;
    int flag;
    ORDER_AND_ERROR * o_and_e;
    LEVEL * level;
    ORDERNODE * ordernode;
    int price;
    int id;
    int dir;
    int orderType;
    char * ts;
    char buildup[MAXSTRING];
    char part[MAXSTRING];
    int bid;
    int ask;
    int bidSize;
    int bidDepth;
    int askSize;
    int askDepth;
    int64_t nav64;
    ACCOUNT * account;
    
    assert(argc == 3);
    
    bk_strlcpy(Venue, argv[1], SMALLSTRING);
    bk_strlcpy(Symbol, argv[2], SMALLSTRING);
    
    while (1)
    {
        eofcheck = fgets(input, MAXSTRING, stdin);
        
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
                bk_strlcpy(tokens[n], tmp, SMALLSTRING);
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
            ts = new_timestamp();
            printf("{\"ok\": true, \"venue\": \"%s\", \"symbol\": \"%s\", \"ts\": \"%s\",\n", Venue, Symbol, ts);
            free(ts);
            
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
        
        // ----------------------------------------- QUOTE ---------------------------------------------------------------
        
        if (strcmp("QUOTE", tokens[0]) == 0)
        {
            // Quotes are currently hideously inefficient, generated from scratch each time. Could FIXME.
            
            ts = new_timestamp();

            bidSize = get_size_from_level(FirstBidLevel);
            bidDepth = get_depth(FirstBidLevel);
            askSize = get_size_from_level(FirstAskLevel);
            askDepth = get_depth(FirstAskLevel);
            
            // Add all the fields that are always present...
            snprintf(buildup, MAXSTRING, "{\"ok\": true, \"symbol\": \"%s\", \"venue\": \"%s\", \"bidSize\": %d, "
                                         "\"askSize\": %d, \"bidDepth\": %d, \"askDepth\": %d, \"quoteTime\": \"%s\"",
                     Symbol, Venue, bidSize, askSize, bidDepth, askDepth, ts);
            
            if (FirstBidLevel)
            {
                bid = FirstBidLevel->price;
                snprintf(part, MAXSTRING, ", \"bid\": %d", bid);
                strncat(buildup, part, MAXSTRING - strlen(buildup) - 1);
            }

            if (FirstAskLevel)
            {
                ask = FirstAskLevel->price;
                snprintf(part, MAXSTRING, ", \"ask\": %d", ask);
                strncat(buildup, part, MAXSTRING - strlen(buildup) - 1);
            }
            
            if (LastTradeTime)
            {
                snprintf(part, MAXSTRING, ", \"lastTrade\": \"%s\", \"lastSize\": %d, \"last\": %d", LastTradeTime, LastSize, LastPrice);
                strncat(buildup, part, MAXSTRING - strlen(buildup) - 1);
            }

            strncat(buildup, "}", MAXSTRING - strlen(buildup) - 1);
            
            printf("%s", buildup);
            
            free(ts);
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
        
        // --------------------------------------- DEBUG REQUEST FOR MEMORY INFO -----------------------------------------
        
        if (strcmp("__DEBUG_MEMORY__", tokens[0]) == 0)
        {
            printf( "DebugInfo.inits_of_level: %d,\n"                // The compiler auto-concatenates these things
                    "DebugInfo.inits_of_fill: %d,\n"                 // (note the lack of commas)
                    "DebugInfo.inits_of_fillnode: %d,\n" 
                    "DebugInfo.inits_of_order: %d,\n"
                    "DebugInfo.inits_of_ordernode: %d,\n"
                    "DebugInfo.inits_of_account: %d,\n"
                    "DebugInfo.reallocs_of_global_order_list: %d,\n"
                    "DebugInfo.reallocs_of_global_account_list: %d,\n"
                    "DebugInfo.reallocs_of_account_order_list: %d",
                    DebugInfo.inits_of_level,
                    DebugInfo.inits_of_fill,
                    DebugInfo.inits_of_fillnode,
                    DebugInfo.inits_of_order,
                    DebugInfo.inits_of_ordernode,
                    DebugInfo.inits_of_account,
                    DebugInfo.reallocs_of_global_order_list,
                    DebugInfo.reallocs_of_global_account_list,
                    DebugInfo.reallocs_of_account_order_list
                    );
            end_message();
            continue;
        }
        
        // ---------------------------------------- SCORES ---------------------------------------------------------------
        
        if (strcmp("__SCORES__", tokens[0]) == 0)
        {
            printf("<pre>%s %s\n", Venue, Symbol);

            if (LastPrice == -1)
            {
                printf("No trading activity yet.</pre>");
                end_message();
                continue;
            }
            
            printf("Current price: $%d.%02d\n\n", LastPrice / 100, LastPrice % 100);
            
            printf("             Account           USD $          Shares         Pos.min         Pos.max           NAV $\n");
            
            for (n = 0; n < CurrentAccountArrayLen; n++)
            {
                if (AllAccounts[n])
                {
                    account = AllAccounts[n];
                    
                    nav64 = account->cents + account->shares * LastPrice;
                    if (nav64 > 2147483647) nav64 = 2147483647;
                    if (nav64 < -2147483647) nav64 = -2147483647;
                    
                    printf("%20s %15d %15d %15d %15d %15d\n",
                           account->name, account->cents / 100, account->shares, account->posmin, account->posmax, (int) nav64 / 100);
                }
            }
            printf("</pre>");
            
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
