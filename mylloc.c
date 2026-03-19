#define _GNU_SOURCE
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include "mylloc.h"

#define ARENA_SIZE        (2 * 1024 * 1024) 
#define MMAP_THRESHOLD    (128 * 1024)
#define ALIGNMENT         16
#define NUM_BINS          64
#define MIN_CHUNK_SIZE    32 // 16 for header+footer (Chunk) + 16 for payload

#define TAG_IN_USE        0x1
#define TAG_IS_MAPPED     0x2
#define TAG_MASK          (~(uintptr_t)0xF)

typedef uintptr_t tag_t;

typedef struct Chunk_st {
    tag_t header;
    union {
        struct {
            struct Chunk_st *next;
            struct Chunk_st *prev;
        } free_list;      
        char payload[0];  
    };
} Chunk;

typedef struct Arena {
    struct Arena *next;
    size_t total_size;
} Arena;

static Arena* global_arenas = NULL;
static Chunk* bins[NUM_BINS];
static pthread_mutex_t alloc_mutex = PTHREAD_MUTEX_INITIALIZER;


static inline size_t get_size(tag_t tag) { return tag & TAG_MASK; }
static inline int is_used(tag_t tag) { return (int)(tag & TAG_IN_USE); }

static inline tag_t* get_footer_ptr(Chunk *c) {
    return (tag_t*)((char*)c + get_size(c->header) - sizeof(tag_t));
}

static inline void set_tags(Chunk *c, size_t size, int used, int mapped) {
    tag_t tag = size | (used ? TAG_IN_USE : 0) | (mapped ? TAG_IS_MAPPED : 0);
    c->header = tag;
    *get_footer_ptr(c) = tag;
}

static inline Chunk* get_next_chunk(Chunk *c) {
    return (Chunk*)((char*)c + get_size(c->header));
}

static inline Chunk* get_prev_chunk(Chunk *c) {
    tag_t* prev_footer = (tag_t*)((char*)c - sizeof(tag_t));
    return (Chunk*)((char*)c - get_size(*prev_footer));
}

static inline size_t align_up(size_t size) {
    return (size + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);
}

/* --- Bins --- */

static int get_bin_index(size_t size) {
    if (size <= 512) return (size / 16) - 1;
    int idx = 32 + (31 - __builtin_clz((uint32_t)size)) - 9;
    return (idx >= NUM_BINS) ? NUM_BINS - 1 : (idx < 0 ? 0 : idx);
}

static void bin_insert(Chunk *c) {
    int idx = get_bin_index(get_size(c->header));
    c->free_list.next = bins[idx];
    c->free_list.prev = NULL;
    if (bins[idx]) bins[idx]->free_list.prev = c;
    bins[idx] = c;
}

static void bin_remove(Chunk *c) {
    int idx = get_bin_index(get_size(c->header));
    if (c->free_list.prev) c->free_list.prev->free_list.next = c->free_list.next;
    else bins[idx] = c->free_list.next;
    
    if (c->free_list.next) c->free_list.next->free_list.prev = c->free_list.prev;
}

/* --- Arena --- */

static Chunk* request_new_arena(size_t size) {
    size_t req_size = (size > ARENA_SIZE) ? align_up(size + MIN_CHUNK_SIZE * 2 + sizeof(Arena)) : ARENA_SIZE;
    void *ptr = mmap(NULL, req_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) return NULL;

    Arena* a = (Arena*)ptr;
    a->total_size = req_size;
    a->next = global_arenas;
    global_arenas = a;

    size_t arena_header_offset = align_up(sizeof(Arena));

    // first guard chunk
    Chunk *start_guard = (Chunk *)((char *)ptr + arena_header_offset);
    set_tags(start_guard, MIN_CHUNK_SIZE, 1, 0);

    Chunk *main_chunk = get_next_chunk(start_guard);
    
    size_t overhead = (char*)main_chunk - (char*)ptr + MIN_CHUNK_SIZE; 
    size_t usable = req_size - overhead;
    
    set_tags(main_chunk, usable, 0, 0);

    // second guard chunk
    Chunk *end_guard = get_next_chunk(main_chunk);
    set_tags(end_guard, MIN_CHUNK_SIZE, 1, 0);

    return main_chunk;
}

/* --- Malloc & Free --- */

void* mylloc(size_t size) {
    if (size == 0) return NULL;

    if (size > MMAP_THRESHOLD) {
        size_t total_size = align_up(size + sizeof(tag_t) * 2);
        Chunk *c = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (c == MAP_FAILED) return NULL;
        set_tags(c, total_size, 1, 1);
        return c->payload;
    }

    size_t actual_size = align_up(size + sizeof(tag_t) * 2);
    if (actual_size < MIN_CHUNK_SIZE) actual_size = MIN_CHUNK_SIZE;

    pthread_mutex_lock(&alloc_mutex);

    int start_bin = get_bin_index(actual_size);
    Chunk* found = NULL;
    for (int i = start_bin; i < NUM_BINS; i++) {
        Chunk *curr = bins[i];
        while (curr) {
            if (get_size(curr->header) >= actual_size) {
                found = curr;
                goto found_it; // могу себе позволить
            }
            curr = curr->free_list.next;
        }
    }

found_it:
    if (!found) {
        found = request_new_arena(actual_size);
        if (!found) { pthread_mutex_unlock(&alloc_mutex); return NULL; }
    } else {
        bin_remove(found);
    }

    size_t total_sz = get_size(found->header);
    if (total_sz >= actual_size + 64) {
        set_tags(found, actual_size, 1, 0);
        Chunk *rem = get_next_chunk(found);
        set_tags(rem, total_sz - actual_size, 0, 0);
        bin_insert(rem);
    } else {
        set_tags(found, total_sz, 1, 0);
    }

    pthread_mutex_unlock(&alloc_mutex);
    return found->payload;
}

void myfree(void *ptr) {
    if (!ptr) return;

    Chunk *c = (Chunk *)((char *)ptr - offsetof(Chunk, payload));
    tag_t tag = c->header;

    if (tag & TAG_IS_MAPPED) {
        munmap(c, get_size(tag));
        return;
    }

    pthread_mutex_lock(&alloc_mutex);

    // forward coalescing
    Chunk *next = get_next_chunk(c);
    if (!is_used(next->header)) {
        bin_remove(next);
        set_tags(c, get_size(c->header) + get_size(next->header), 0, 0);
    }

    // backward coalescing
    tag_t *prev_footer = (tag_t *)((char *)c - sizeof(tag_t));
    if (!is_used(*prev_footer)) {
        Chunk *prev = get_prev_chunk(c);
        bin_remove(prev);
        size_t new_sz = get_size(prev->header) + get_size(c->header);
        c = prev;
        set_tags(c, new_sz, 0, 0);
    }

    set_tags(c, get_size(c->header), 0, 0);
    bin_insert(c);

    pthread_mutex_unlock(&alloc_mutex);
}
