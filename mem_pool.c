
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;

/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;

/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;

/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);

/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    //verify that mem_init() is only called once for every mem_fee()
    if(pool_store != NULL){
        return ALLOC_CALLED_AGAIN;
    };

    // allocate the pool store with initial capacity
    pool_store = calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
    pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
    return ALLOC_OK;
}

alloc_status mem_free() {
    // verify that mem_free() is called only once for each mem_init()
    if(pool_store == NULL){
        return ALLOC_CALLED_AGAIN;
    };

    // verify all pool managers have been deallocated
    for(int i=0;i<pool_store_size;i++){
        if(pool_store[i] != NULL){
            mem_pool_close((pool_pt) pool_store[i]);
        };
    }

    // free pool store and static variables
    free(pool_store);
    pool_store = NULL;
    pool_store_size = 0;
    pool_store_capacity = 0;
    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {

    // verify that the pool store is allocated
    if(pool_store == NULL){
        return NULL;
    };

    // resize the pool store if nessisary
    _mem_resize_pool_store();

    // allocate a new mem pool mgr
    pool_mgr_t *pool_manager;
    pool_manager = calloc(1,sizeof(pool_mgr_t));

    // ccheck if pool mgr was allocated properly
    if(pool_manager == NULL){
        return NULL;
    };

    // allocate a new memory pool
    pool_manager->pool.mem = malloc(size);

    // check if pool was allocated properly, if not, deallocate pool mgr
    if(pool_manager->pool.mem == NULL){
        free(pool_manager);
        return NULL;
    };


    // allocate a new node heap
    pool_manager->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    pool_manager->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;

    // check if node heap was allocated properly, if not, deallocate pool mgr and pool
    if(pool_manager->node_heap == NULL){
        free(pool_manager->pool.mem);
        free(pool_manager);
        return NULL;
    };

    // allocate a new gap index
    pool_manager->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY,sizeof(gap_t));
    pool_manager->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    // check if gap index was allocated properly, if not, deallocate pool mgr, node heap, and pool
    if(pool_manager->gap_ix == NULL){
        free(pool_manager->pool.mem);
        free(pool_manager->node_heap);
        free(pool_manager);
        return NULL;
    };

    //initialize the pool
    pool_manager->pool.total_size = size;
    pool_manager->pool.alloc_size = 0;
    pool_manager->pool.policy = policy;
    pool_manager->pool.num_gaps = 1;
    pool_manager->pool.num_allocs = 0;

    //  initialize top node of node heap
    pool_manager->node_heap[0].next = NULL;
    pool_manager->node_heap[0].prev = NULL;
    pool_manager->node_heap[0].allocated = 0;
    pool_manager->node_heap[0].used = 1;
    pool_manager->node_heap[0].alloc_record.size = pool_manager->pool.total_size;
    pool_manager->node_heap[0].alloc_record.mem = pool_manager->pool.mem;
    pool_manager->used_nodes = 1;

    //   initialize top node of gap index
    pool_manager->gap_ix[0].node = &pool_manager->node_heap[0];

    //   assign pool manager to the pool store array index
    pool_store[pool_store_size] = pool_manager;
    pool_store_size++;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_manager;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // verify that the pool is allocated
    if(pool_mgr->pool.mem == NULL){
        // not allocated
    };

    // verify if the pool has one gap
    if(pool_mgr->gap_ix->size == 1){
        // only one gap
    }

    // verify that the pool has more than zero allocations
    if(pool_mgr->pool.num_allocs != 0){
        return ALLOC_NOT_FREED;
        // no allocations
    }

    // free memory pool
    free(pool_mgr->pool.mem);

    // free node heap
    free(pool_mgr->node_heap);

    // free gap index
    free(pool_mgr->gap_ix);

    // locate pool mgr in pool store array and set to null
    for(int i = 0;i<pool_store_capacity - 1;i++){
        if(pool_store[i] == pool_mgr){
            pool_store[i] = NULL;
        };
    };

    // free pool manager
    free(pool_mgr);

    return ALLOC_OK;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // verify if there are any gaps in the pool
    if (pool_mgr->pool.num_gaps == 0) {
        return NULL;
    };

    // resize node heap if nessisary
    _mem_resize_node_heap(pool_mgr);

    // check if used nodes are greater than total nodes
    if (pool_mgr->used_nodes >= pool_mgr->total_nodes) {
        return NULL;
    };



    // if FIRST_FIT, then find the first unallocated node with appropriate size in the node heap
    node_pt alloc_node;
    if (pool_mgr->pool.policy == FIRST_FIT) {
        for (int i = 0; i < pool_mgr->total_nodes; i++) {
            if (pool_mgr->node_heap[i].alloc_record.size >= size && pool_mgr->node_heap[i].allocated == 0) {
                alloc_node = &pool_mgr->node_heap[i];
                break;
            }else{
                alloc_node = NULL;
            };
        }
    };

    // if BEST_FIT, then find the first gap node with appropriate size in the gap index
    if(pool_mgr->pool.policy == BEST_FIT){
        for (int i = 0; i < pool_mgr->pool.num_gaps; i++) {
            if (pool_mgr->gap_ix[i].node->alloc_record.size >= size && pool_mgr->gap_ix[i].node->allocated == 0) {
                alloc_node = pool_mgr->gap_ix[i].node;
                break;
            }else{
                alloc_node = NULL;
            };
        }
    };

    // check if node found
    if (alloc_node == NULL) {
        return NULL;
    };

    // incriment num_allocs and add the size of the new allocation to alloc_size
    pool->num_allocs++;
    pool->alloc_size += size;

    // calculate the size of the remaining gap
    size_t gap_remain;
    gap_remain = alloc_node->alloc_record.size - size;

    // remove node from gap index
    _mem_remove_from_gap_ix(pool_mgr, size, alloc_node);

    // update the variables of the new allocated node
    alloc_node->allocated = 1;
    alloc_node->alloc_record.size = size;


    //use new node to handle remaining gap
    node_pt gap_node;

    // locate an unused node in the node heap
    if(gap_remain != 0) {
        for (int i = 0; i < pool_mgr->total_nodes; i++) {
            if (pool_mgr->node_heap[i].used == 0) {
                gap_node = &pool_mgr->node_heap[i];
                break;
            }
        }

        //   verify that the node we found exists
        if (gap_node == NULL) {
            //gap not found!
        }

        //   initialize the gap node by adding the appropriate values
        gap_node->alloc_record.size = gap_remain;
        gap_node->alloc_record.mem = alloc_node->alloc_record.mem + size;
        gap_node->used = 1;
        gap_node->allocated = 0;

        //  incriment the used nodes of the pool manager
        pool_mgr->used_nodes++;

        //   set new allocated node to point next to the remaining gap node
        if (alloc_node->next == NULL) {
            alloc_node->next = gap_node;
            gap_node->next = NULL;
            gap_node->prev = alloc_node;
        }
        else {
            gap_node->next = alloc_node->next;
            alloc_node->next->prev = gap_node;
            alloc_node->next = gap_node;
            gap_node->prev = alloc_node;
        };

        //   add to gap index
        _mem_add_to_gap_ix(pool_mgr, gap_remain, gap_node);
    }

    //   verify that the new allocated node was created succesfully
    if(alloc_node == NULL){
        return NULL;
    };

    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) alloc_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;

    node_pt node_delete;

    // locate the node in the node heap
    for(int i=0;i < pool_mgr->total_nodes; i++){
        if(pool_mgr->node_heap[i].alloc_record.mem == node->alloc_record.mem){

            node_delete = &pool_mgr->node_heap[i];
            break;
        };
    };

    // verify that node is not null
    if(node_delete == NULL){
        return ALLOC_NOT_FREED;
    };

    // set node to gap
    node_delete->allocated = 0;

    // update pool manager variables
    pool_mgr->pool.num_allocs--;
    pool_mgr->pool.alloc_size = pool_mgr->pool.alloc_size - node_delete->alloc_record.size;

    //if the next node is also a gap, merge to one gap
    node_pt merge_node;
    if(node_delete->next != NULL && node_delete->next->allocated == 0 && node_delete->next->used == 1){

        merge_node = node_delete->next;

        //  remove the next node from the gap index
        _mem_remove_from_gap_ix(pool_mgr,merge_node->alloc_record.size,merge_node);

        //  add size to the gap node
        node_delete->alloc_record.size = node_delete->alloc_record.size + node_delete->next->alloc_record.size;

        //  set merged node used variable to zero
        merge_node->used = 0;

        //   deincriment used nodes
        pool_mgr->used_nodes--;

        //  set the merge node next prev pointer to
        //node delete and node delete next to the merge node next
        // or set it to null if the merged next is null
        if (merge_node->next) {
            merge_node->next->prev = node_delete;
            node_delete->next = merge_node->next;
        } else {
            node_delete->next = NULL;
        }
        merge_node->next = NULL;
        merge_node->prev = NULL;
        merge_node->alloc_record.size = 0;
        merge_node->alloc_record.mem = NULL;
    };

    // add the node delete to the gap index
    _mem_add_to_gap_ix(pool_mgr,node_delete->alloc_record.size,node_delete);

    //if the node previous to the node to delete is a gap, merge the node
    node_pt prev_node;
    if (node_delete->prev != NULL && node_delete->prev->used == 1 && node_delete->prev->allocated == 0) {
        prev_node = node_delete->prev;

        //   remove the previous node and node to delete from the gap index
        _mem_remove_from_gap_ix(pool_mgr, prev_node->alloc_record.size, prev_node);
        _mem_remove_from_gap_ix(pool_mgr, node_delete->alloc_record.size,node_delete);



        //   add the node to delete to the previous node
        prev_node->alloc_record.size = node_delete->alloc_record.size + node_delete->prev->alloc_record.size;

        //  clear the metadata from the node to delete
        node_delete->alloc_record.size = 0;
        node_delete->alloc_record.mem = NULL;
        node_delete->used = 0;
        node_delete->allocated = 0;

        //   deincrement used nodes
        pool_mgr->used_nodes--;

        //  set the prev node next pointer to node delete next
        // and the prev pointer from the node delete next to prev node
        if (node_delete->next) {
            prev_node->next = node_delete->next;
            node_delete->next->prev = prev_node;
        } else {
            prev_node->next = NULL;
        }
        node_delete->next = NULL;
        node_delete->prev = NULL;

        //   add the previous node to the gap index
        _mem_add_to_gap_ix(pool_mgr,prev_node->alloc_record.size,prev_node);


    };

    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // allocate the segments array with number of elements as the
    //number of used nodes
    pool_segment_pt segs;
    segs = calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));
    if(segments == NULL){
        return;
    }

    // set each segment size and allocation to the corresponding
    //node size and allocation

    int j = 0;
    node_pt node_loop;
    node_loop = &pool_mgr->node_heap[0];

    while(node_loop){
        segs[j].allocated = node_loop->allocated;
        segs[j].size = node_loop->alloc_record.size;
        node_loop = node_loop->next;
        j++;
    }

    *segments = segs;
    *num_segments = pool_mgr->used_nodes;
    return;
}

/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // resize pool store and capacity if ratio is less than fill factor
    if (((float) pool_store_size / pool_store_capacity)
        > MEM_POOL_STORE_FILL_FACTOR) {
        if((pool_store = realloc(pool_store,
                                 pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR *
                                 sizeof(pool_mgr_pt)))){
            pool_store_capacity *= MEM_POOL_STORE_EXPAND_FACTOR;
            return ALLOC_OK;
        };
    };
    return ALLOC_FAIL;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // resize node heap and capacity if ratio is less than fill factor
    if (((float) pool_mgr->node_heap->used / pool_mgr->node_heap->alloc_record.size) > MEM_NODE_HEAP_FILL_FACTOR){
        if((pool_mgr->node_heap = realloc(pool_mgr->node_heap,
                                          MEM_NODE_HEAP_EXPAND_FACTOR * MEM_NODE_HEAP_INIT_CAPACITY * sizeof(node_t)))){
            pool_mgr->node_heap->alloc_record.size *= MEM_NODE_HEAP_EXPAND_FACTOR;
            return ALLOC_OK;
        };
    };
    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // resize gap index and capacity if ratio is less than fill factor
    if(((float)pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR){

        if((pool_mgr->gap_ix = realloc(pool_mgr->gap_ix,
                                       MEM_GAP_IX_EXPAND_FACTOR * MEM_GAP_IX_INIT_CAPACITY * sizeof(gap_t)))){
            pool_mgr->gap_ix_capacity *= MEM_GAP_IX_EXPAND_FACTOR;
            return ALLOC_OK;
        };
    };
    return ALLOC_FAIL;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {
    // expand the gap index
    _mem_resize_gap_ix(pool_mgr);

    // add the entry at the last slot (total number of gaps)
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;

    // increment number of gaps
    pool_mgr->pool.num_gaps++;

    // check if the gap sort is successful, return ok
    if(_mem_sort_gap_ix(pool_mgr) == ALLOC_OK){
        return ALLOC_OK;
    };

    return ALLOC_FAIL;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {

    // locate the position of the node in the gap index
    int position;
    for(int i=0; i < pool_mgr->pool.num_gaps - 1; i++){
        if(pool_mgr->gap_ix[i].node->alloc_record.mem == node->alloc_record.mem && pool_mgr->gap_ix[i].size == size){
            position = i;
        }
    }

    // loop from the position to the end of the number of gaps
    //set each gap from the slot above to the slot previous
    for(int i=position; i<pool_mgr->pool.num_gaps - 1; i++){
        pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i+1];
    }

    // deincriment the number of gaps
    pool_mgr->pool.num_gaps--;


    //clear the metadata from the position gap
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}


static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // loop from num_gaps - 1 to 1
    //    if the size of the current gap is less than the previous gap, swap them
    for(int i= pool_mgr->pool.num_gaps-1;i > 0; i--){
        if(pool_mgr->gap_ix[i].node->alloc_record.size < pool_mgr->gap_ix[i - 1].node->alloc_record.size
           || pool_mgr->gap_ix[i].node->alloc_record.mem < pool_mgr->gap_ix[i - 1].node->alloc_record.mem
              && pool_mgr->gap_ix[i].node->alloc_record.size == pool_mgr->gap_ix[i - 1].node->alloc_record.size){

            struct _gap temp_gap;
            temp_gap = pool_mgr->gap_ix[i - 1];
            pool_mgr->gap_ix[i - 1] = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = temp_gap;
        };
    };
    return ALLOC_OK;
}
