#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#define BUY 1
#define SELL 2

#define LIMIT 1
#define MARKET 2
#define FOK 3
#define IOC 4

struct Fill_struct {
    int price;
    int qty;
    char * timestamp;
} FILL;

struct FillNode_struct {
    struct Fill_struct * fill;
    struct FillNode_struct * prev;
    struct FillNode_struct * next;
} FILLNODE;

struct Order_struct {
    // char * venue;        // Frontend knows these so we don't need to
    // char * symbol;
    int direction;
    int originalQty;
    int qty;
    int price;
    int orderType;
    int id;
    int account;            // Frontend will map from string to int
    char * timestamp;
    struct FillNode_struct * firstfillnode;
    int totalFilled;
    int open;
} ORDER;

struct OrderNode_struct {
    struct Order_struct * order;
    struct OrderNode_struct * prev;
    struct OrderNode_struct * next;
} ORDERNODE;

struct Level_struct {
    struct Level_struct * prev;
    struct Level_struct * next;
    int price;
    struct OrderNode_struct * firstordernode;
} LEVEL;


// Globals......


LEVEL * FirstBidLevel = NULL;
LEVEL * FirstAskLevel = NULL;

char * LastTradeTime = NULL;

int LastPrice = -1;
int LastSize = -1;



LEVEL * init_level(int price, ORDERNODE * ordernode, LEVEL * prev, LEVEL * next)
{
    LEVEL * ret;
    
    ret = malloc(sizeof(LEVEL));
    assert(ret);
    
    ret->price = price;
    ret->firstordernode = ordernode;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


FILL * init_fill(int price, int qty, char * timestamp)
{
    FILL * ret;
    
    ret = malloc(sizeof(FILL));
    assert(ret);
    
    ret->price = price;
    ret->qty = qty;
    ret->timestamp = timestamp;
    
    return ret;
}


FILLNODE * init_fillnode(FILL * fill, FILLNODE * prev, FILLNODE * next)
{
    FILLNODE * ret;
    
    ret = malloc(sizeof(FILLNODE));
    assert(ret);
    
    ret->fill = fill;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


ORDERNODE * init_ordernode(ORDER * order, ORDERNODE * prev, ORDERNODE * next)
{
    ORDERNODE * ret;
    
    ret = malloc(sizeof(ORDERNODE));
    assert(ret);
    
    ret->order = order;
    ret->prev = prev;
    ret->next = next;
    
    return ret;
}


int new_id(void)
{
    static int id = 0;
    return id++;
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
    mod_strncpy(timestamp, "FIXME", 32);
    
    return timestamp;
}


ORDER * init_order(int account, int qty, int price, int direction, int orderType)
{
    ORDER * ret;
    
    ret = malloc(sizeof(ORDER));
    assert(ret);
    
    ret->direction = direction;
    ret->originalQty = qty;
    ret->qty = qty;
    ret->price = price;
    ret->orderType = orderType;
    ret->id = new_id();
    ret->account = account;
    ret->timestamp = new_timestamp();
    ret->firstfillnode = NULL;
    ret->totalFilled = 0;
    ret->open = 1;

    return ret;
}




void cross(ORDER * standing, ORDER * incoming)
{
    int quantity;
    int price;
    char * timestamp;
    FILLNODE * currentfillnode;
    
    timestamp = new_timestamp()
    
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

    LastTradeTime = timestamp;
    LastPrice = price;
    LastSize = quantity;
    
    fill = init_fill(price, quantity, timestamp);
    assert(fill);

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
    
    // Again for other otder...
    
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
                cross(current_node->order, order;
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
                cross(current_node->order, order;
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
            FirstBidLevel = current_level;
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
            FirstAskLevel = current_level;
            return;
        }
        
        if (current_node->next != NULL)
        {
            old_node = current_node;
            current_node = current_node->next;
            free(old_node);
        } else {
            old_node = current_node
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

    
void insert_ask(order)
{
    ORDERNODE * ordernode;
    
    ordernode = init_ordernode(order, NULL, NULL);      // Fix prev and next later
    
    if (FirstAskLevel == NULL)
    {
        FirstAskLevel = init_level(order->price, ordernode, NULL, NULL);
        return;
    }
    
    // in progress
}


ORDER * parse_order(int account, int qty, int price, int direction, int orderType)
{
    ORDER * order;
    order = init_order(account, qty, price, direction, orderType);
    
    run_order(order);
    
    if (order->direction == SELL)
    {
        cleanup_closed_bids();
    } else {
        cleanup_closed_asks();
    }
    
    if (order->open)
    {
        if (order->type == limit)
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
    
    return order;
}


int main(void)
{
    return 0;
}