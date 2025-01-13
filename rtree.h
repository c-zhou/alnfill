// Copyright 2023 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

#ifndef RTREE_H
#define RTREE_H

#include <stdlib.h>
#include <stdbool.h>
#include <limits.h>

////////////////////////////////
/***
#define DATATYPE void *
#define DIMS 2
#define NUMTYPE double
#define NUMINF INFINITY
#define MAXITEMS 64
**/

typedef signed long long int64;
#define DATATYPE void *
#define DIMS 2
#define NUMTYPE int64
#define NUMINF LLONG_MAX
#define MAXITEMS 16


// used for splits
#define MINITEMS_PERCENTAGE 10
#define MINITEMS ((MAXITEMS) * (MINITEMS_PERCENTAGE) / 100 + 1)

#ifndef RTREE_NOPATHHINT
#define USE_PATHHINT
#endif

#ifdef RTREE_MAXITEMS
#undef MAXITEMS
#define MAXITEMS RTREE_MAXITEMS
#endif

#ifdef RTREE_NOATOMICS
typedef int rc_t;
static int rc_load(rc_t *ptr, bool relaxed) {
    (void)relaxed; // nothing to do
    return *ptr;
}
static int rc_fetch_sub(rc_t *ptr, int val) {
    int rc = *ptr;
    *ptr -= val;
    return rc;
}
static int rc_fetch_add(rc_t *ptr, int val) {
    int rc = *ptr;
    *ptr += val;
    return rc;
}
#else 
#include <stdatomic.h>
typedef atomic_int rc_t;
static int rc_load(rc_t *ptr, bool relaxed) {
    if (relaxed) {
        return atomic_load_explicit(ptr, memory_order_relaxed);
    } else {
        return atomic_load(ptr);
    }
}
static int rc_fetch_sub(rc_t *ptr, int delta) {
    return atomic_fetch_sub(ptr, delta);
}
static int rc_fetch_add(rc_t *ptr, int delta) {
    return atomic_fetch_add(ptr, delta);
}
#endif

enum kind {
    LEAF = 1,
    BRANCH = 2,
};

struct rect {
    NUMTYPE min[DIMS];
    NUMTYPE max[DIMS];
};

struct item {
    const DATATYPE data;
};

struct node {
    rc_t rc;            // reference counter for copy-on-write
    enum kind kind;     // LEAF or BRANCH
    int count;          // number of rects
    struct rect rects[MAXITEMS];
    union {
        struct node *nodes[MAXITEMS];
        struct item datas[MAXITEMS];
    };
};

struct rtree {
    struct rect rect;
    struct node *root;
    size_t count;
    size_t height;
#ifdef USE_PATHHINT
    int path_hint[16];
#endif
    bool relaxed;
    void *(*malloc)(size_t);
    void (*free)(void *);
    void *udata;
    bool (*item_clone)(const DATATYPE item, DATATYPE *into, void *udata);
    void (*item_free)(const DATATYPE item, void *udata);
};

////////////////////////////////

// rtree_new returns a new rtree
//
// Returns NULL if the system is out of memory.
struct rtree *rtree_new(void);

void rtree_init(struct rtree *tr);

// rtree_new returns a new rtree using a custom allocator
//
// Returns NULL if the system is out of memory.
struct rtree *rtree_new_with_allocator(void *(*malloc)(size_t), void (*free)(void*));

void rtree_init_with_allocator(struct rtree *tr, void *(*malloc)(size_t), void (*free)(void*));

// rtree_free frees an rtree
void rtree_free(struct rtree *tr);
void rtree_free_nodes(struct rtree *tr);

// rtree_clone makes an instant copy of the btree.
//
// This operation uses shadowing / copy-on-write.
struct rtree *rtree_clone(struct rtree *tr);

// rtree_set_item_callbacks sets the item clone and free callbacks that will be
// called internally by the rtree when items are inserted and removed.
//
// These callbacks are optional but may be needed by programs that require
// copy-on-write support by using the rtree_clone function.
//
// The clone function should return true if the clone succeeded or false if the
// system is out of memory.
void rtree_set_item_callbacks(struct rtree *tr,
    bool (*clone)(const void *item, void **into, void *udata), 
    void (*free)(const void *item, void *udata));

// rtree_set_udata sets the user-defined data. 
//
// This should be called once after rtree_new() and is only used for 
// the item callbacks as defined in rtree_set_item_callbacks().
void rtree_set_udata(struct rtree *tr, void *udata);

// rtree_insert inserts an item into the rtree. 
//
// This operation performs a copy of the data that is pointed to in the second
// and third arguments. The R-tree expects a rectangle, which is two arrays of
// doubles. The first N values as the minimum corner of the rect, and the next
// N values as the maximum corner of the rect, where N is the number of
// dimensions.
//
// When inserting points, the max coordinates is optional (set to NULL).
//
// Returns false if the system is out of memory.
bool rtree_insert(struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max, const void *data);


// rtree_search searches the rtree and iterates over each item that intersect
// the provided rectangle.
//
// Returning false from the iter will stop the search.
void rtree_search(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max,
    bool (*iter)(const NUMTYPE *min, const NUMTYPE *max, const void *data, void *udata), 
    void *udata);

// rtree_search_inside searches the rtree and iterates over each item that is
// contained by the provided rectangle.
//
// Returning false from the iter will stop the search.
void rtree_search_inside(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max,
    bool (*iter)(const NUMTYPE *min, const NUMTYPE *max, const void *data, void *udata), 
    void *udata);

// rtree_count_node_contains searches the rtree and counts the item that contains
// the provided rectangle.
int rtree_count_node_contains(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max);

// rtree_exist_node_contains searches the rtree and check if there exists an item that contains
// the provided rectangle.
bool rtree_exist_node_contains(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max);

// rtree_count_node_inside searches the rtree and counts the item that is contained by
// the provided rectangle.
int rtree_count_node_inside(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max);

// rtree_exist_node_inside searches the rtree and check if there exists an item that is contained by
// the provided rectangle.
bool rtree_exist_node_inside(const struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max);

// rtree_scan iterates over every item in the rtree.
//
// Returning false from the iter will stop the scan.
void rtree_scan(const struct rtree *tr,
    bool (*iter)(const NUMTYPE *min, const NUMTYPE *max, const void *data, void *udata), 
    void *udata);

// rtree_count returns the number of items in the rtree.
size_t rtree_count(const struct rtree *tr);

// rtree_delete deletes an item from the rtree. 
//
// This searches the tree for an item that is contained within the provided
// rectangle, and perform a binary comparison of its data to the provided
// data. The first item that is found is deleted.
//
// Returns false if the system is out of memory.
bool rtree_delete(struct rtree *tr, const NUMTYPE *min, const NUMTYPE *max, const void *data);

// rtree_delete_with_comparator deletes an item from the rtree.
// This searches the tree for an item that is contained within the provided
// rectangle, and perform a comparison of its data to the provided data using
// a compare function. The first item that is found is deleted.
//
// Returns false if the system is out of memory.
bool rtree_delete_with_comparator(struct rtree *tr, const NUMTYPE *min, 
    const NUMTYPE *max, const void *data,
    int (*compare)(const void *a, const void *b, void *udata),
    void *udata);

// rtree_opt_relaxed_atomics activates memory_order_relaxed for all atomic
// loads. This may increase performance for single-threaded programs.
// Optionally, define RTREE_NOATOMICS to disbale all atomics.
void rtree_opt_relaxed_atomics(struct rtree *tr);

#endif // RTREE_H
