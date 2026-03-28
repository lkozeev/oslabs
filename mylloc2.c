#define _GNU_SOURCE
#include <stdint.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include "mylloc.h"

#include <stdio.h>

#define MYLLOC_TEST


#define ARENA_SIZE        ((size_t)2 * 1024 * 1024) 
#define MMAP_THRESHOLD    ((size_t)128 * 1024)
#define ALIGNMENT         (size_t)16
#define NUM_BINS          (size_t)64
#define MIN_CHUNK_SIZE    (size_t)32 // 16 for header+footer (Chunk) + 16 for payload
#define MAX_ALLOC_SIZE    ((size_t)1 << 31)

#define TAG_IN_USE        (size_t)0b001
#define TAG_IS_MAPPED     (size_t)0b010
#define TAG_IS_GUARD      (size_t)0b100
#define TAG_SIZE          (size_t)0 // там можно, поскольку size выровнен по 16
#define TAG_MASK          (size_t)0b111 // маска нижних битов size для флагов

typedef size_t tag_t; // в целом тип не важен, важно чтоб целочисленный 8ми байтовый.

typedef struct Chunk {
    tag_t header;
    union {
        struct {
            struct Chunk* next;
            struct Chunk* prev;
        } free_list;      
        char payload[0];  
    };
} Chunk;

typedef struct Arena {
    struct Arena* next;
    struct Arena* prev;
    size_t total_size;
} Arena;

static Arena* global_arenas = NULL;
static Chunk* bins[NUM_BINS];
static pthread_mutex_t alloc_mutex = PTHREAD_MUTEX_INITIALIZER;


static inline size_t get_size(tag_t tag) { return (tag >> TAG_SIZE) & ~TAG_MASK; }
static inline int is_used(tag_t tag) { return (int)(tag & TAG_IN_USE); }
static inline int is_mapped(tag_t tag) { return (int)(tag & TAG_IS_MAPPED); }
static inline int is_guard(tag_t tag) { return (int)(tag & TAG_IS_GUARD); }

static inline tag_t* get_footer_ptr(Chunk *c) {
    return (tag_t*)((char*)c + get_size(c->header) - sizeof(tag_t));
}

static inline void set_tags(Chunk *c, size_t size, int used, int mapped, int is_guard) {
    tag_t tag = (size << TAG_SIZE) | (used ? TAG_IN_USE : 0) | (mapped ? TAG_IS_MAPPED : 0) | (is_guard ? TAG_IS_GUARD : 0) ;
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

static inline size_t align_up(size_t size) { // only 2power
    return (size + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1);
}

/* --- Bins --- */

static int get_bin_index(size_t size) {
    if (size <= 512) return (size / 16) - 1;
    int idx = (63 - __builtin_clzll(size)) - (63 - __builtin_clzll(512)) + 512 / 16;
    // int idx = 32 + (31 - __builtin_clz((uint32_t)size)) - 9;
    return (idx >= NUM_BINS) ? NUM_BINS - 1 : (idx < 0 ? 0 : idx);
}

static void bin_insert(Chunk* c) {
    int idx = get_bin_index(get_size(c->header));
    c->free_list.next = bins[idx];
    c->free_list.prev = NULL;
    if (bins[idx]) bins[idx]->free_list.prev = c;
    bins[idx] = c;
}

static void bin_remove(Chunk* c) {
    int idx = get_bin_index(get_size(c->header));
    if (c->free_list.prev)
        c->free_list.prev->free_list.next = c->free_list.next;
    else
        bins[idx] = c->free_list.next;
    
    if (c->free_list.next)
        c->free_list.next->free_list.prev = c->free_list.prev;
}

/* --- Arena --- */

static Chunk* request_new_arena(size_t size, Arena** out_arena) {
    size_t req_size = (size > ARENA_SIZE) ? align_up(size + MIN_CHUNK_SIZE * 2 + sizeof(Arena)) : ARENA_SIZE;
    #ifdef MYLLOC_TEST
    printf("Requesting new arena for size: %zu\n", req_size);
    #endif
    void* ptr = mmap(NULL, req_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED)
        return NULL;

    Arena* a = (Arena*)ptr;
    a->total_size = req_size;
    *out_arena = a;

    size_t arena_header_offset = align_up(sizeof(Arena) + offsetof(Chunk, payload)) - offsetof(Chunk, payload); // обеспечиваем выравнивание относительно payload
    // first guard chunk
    Chunk* start_guard = (Chunk*)((char*)ptr + arena_header_offset);
    set_tags(start_guard, MIN_CHUNK_SIZE, 1, 0, 1);

    Chunk* main_chunk = get_next_chunk(start_guard);
    
    size_t overhead = (char*)main_chunk - (char*)ptr + MIN_CHUNK_SIZE; 
    size_t usable = req_size - overhead;
    
    set_tags(main_chunk, usable, 0, 0, 0);

    // second guard chunk
    Chunk* end_guard = get_next_chunk(main_chunk);
    set_tags(end_guard, MIN_CHUNK_SIZE, 1, 0, 1);

    return main_chunk;
}

/* --- Malloc & Free --- */

Chunk* find_chunk(size_t size) {
    int start_bin = get_bin_index(size);
    for (int i = start_bin; i < NUM_BINS; i++) {
        Chunk* curr = bins[i];
        while (curr) {
            if (get_size(curr->header) >= size) {
                return curr;
            }
            curr = curr->free_list.next;
        }
    }
    return NULL;
}

static Chunk* split_chunk(Chunk *c, size_t actual_size) {
    size_t total_sz = get_size(c->header);
        if (total_sz >= actual_size + MIN_CHUNK_SIZE) {
        set_tags(c, actual_size, 1, 0, 0);
        Chunk* rem = get_next_chunk(c);
        set_tags(rem, total_sz - actual_size, 0, 0, 0);
        return rem;
    } else {
        set_tags(c, total_sz, 1, 0, 0);
        return NULL;
    }
}

void* mylloc(size_t size) {
    if (size == 0 || size > MAX_ALLOC_SIZE)
        return NULL;

    if (size > MMAP_THRESHOLD) {
        size_t total_size = align_up(size + offsetof(Chunk, payload) + sizeof(tag_t));
        Chunk* c = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (c == MAP_FAILED)
            return NULL;
        set_tags(c, total_size, 1, 1, 0);
        return c->payload;
    }

    size_t actual_size = align_up(size + offsetof(Chunk, payload) + sizeof(tag_t));
    if (actual_size < MIN_CHUNK_SIZE) actual_size = MIN_CHUNK_SIZE;

    pthread_mutex_lock(&alloc_mutex);
    Chunk* found = find_chunk(actual_size);

    if (found) {
        bin_remove(found);
        Chunk* rem = split_chunk(found, actual_size);
        if (rem) bin_insert(rem);

        pthread_mutex_unlock(&alloc_mutex);
        return found->payload;
    }

    pthread_mutex_unlock(&alloc_mutex);

    Arena* new_arena = NULL;
    found = request_new_arena(actual_size, &new_arena);
    if (!found)
        return NULL;
    Chunk* rem = split_chunk(found, actual_size);

    pthread_mutex_lock(&alloc_mutex);
    new_arena->next = global_arenas;
    new_arena->prev = NULL;
    if (global_arenas) global_arenas->prev = new_arena;
    global_arenas = new_arena;
    if (rem) bin_insert(rem);
    pthread_mutex_unlock(&alloc_mutex);
    return found->payload;
}

void myfree(void *ptr) {
    if (!ptr) return;

    Chunk* c = (Chunk*)((char*)ptr - offsetof(Chunk, payload));
    tag_t tag = c->header;
    if (!is_used(tag)) return; // double free (почти бесполезная проверка, поскольку чанки могут сливаться)

    if (is_mapped(tag)) {
        munmap(c, get_size(tag));
        return;
    }

    pthread_mutex_lock(&alloc_mutex);

    // forward coalescing
    Chunk* next = get_next_chunk(c);
    if (!is_used(next->header)) {
        bin_remove(next);
        set_tags(c, get_size(c->header) + get_size(next->header), 0, 0, 0);
    }

    // backward coalescing
    tag_t* prev_footer = (tag_t*)((char*)c - sizeof(tag_t));
    if (!is_used(*prev_footer)) {
        Chunk* prev = get_prev_chunk(c);
        bin_remove(prev);
        set_tags(prev, get_size(prev->header) + get_size(c->header), 0, 0, 0);
        c = prev;
    }

    
    next = get_next_chunk(c);
    prev_footer = (tag_t*)((char*)c - sizeof(tag_t));
    if (is_guard(next->header) && is_guard(*prev_footer)) {
        size_t arena_header_offset = align_up(sizeof(Arena) + offsetof(Chunk, payload)) - offsetof(Chunk, payload);
        Arena* arena = (Arena*)((char*)get_prev_chunk((Chunk*)((char*)c)) - arena_header_offset);
        if (arena->prev) arena->prev->next = arena->next;
        if (arena->next) arena->next->prev = arena->prev;
        if (arena == global_arenas) global_arenas = arena->next;
        #ifdef MYLLOC_TEST
        printf("Unmapping arena of size %zu\n", arena->total_size);
        #endif
        pthread_mutex_unlock(&alloc_mutex);
        munmap(arena, arena->total_size);
        return;
    }

    set_tags(c, get_size(c->header), 0, 0, 0);
    bin_insert(c);
    pthread_mutex_unlock(&alloc_mutex);
}

#ifdef MYLLOC_TEST
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

void arena_stable(const size_t num, const size_t max_reg, const char* stat_name) {
    size_t reg_i = 0;
    void** region = mylloc(max_reg * sizeof(void*));
    size_t total_alloc = 0;
    for (int i = 0; i < num; i++) {
        size_t size = rand() % 1000 + 1;
        void* p = mylloc(size);
        if (p) {
            if ((size_t)p % ALIGNMENT != 0) {
                fprintf(stderr, "Misaligned pointer: %p\n", p);
            }
            memset(p, 1, size);
            if (rand() % 101 == 0) {
                myfree(p);
            } else {
                if (reg_i < max_reg) {
                    total_alloc += get_size(((Chunk*)((char*)p - offsetof(Chunk, payload)))->header);
                    region[reg_i++] = p;
                } else {
                    myfree(p);
                }
            }

            if(rand() % 100 > 0 && reg_i > 10) {
                size_t idx = rand() % reg_i;
                total_alloc -= get_size(((Chunk*)((char*)region[idx] - offsetof(Chunk, payload)))->header);
                myfree(region[idx]);
                region[idx] = region[--reg_i];
                
            }
            if (i % 1000000 == 0) printf("%s: %zu (%zu)\n", stat_name, total_alloc, reg_i);

        } else {
            fprintf(stderr, "Allocation failed at iteration %d\n", i);
        }
    }
}

void stable_test() {
    printf("---arena---\n");
    arena_stable(100000000, 100000, "arena");

    printf("\n---huge---\n");
    for (int i = 0; i < 1000; i++) {
        size_t size = rand() % (1024 * 1024) + 256 * 1024;
        void *p = mylloc(size); // large allocation
        if (p) {
            memset(p, 1, size);
            myfree(p);
        } else {
            fprintf(stderr, "Large allocation failed at iteration %d\n", i);
        }
    }
}

void* stable_multithreaded(void* arg) {
    printf("Thread %s started\n", (const char*)arg);
    arena_stable(20000000, 10000, (const char*)arg);

    return NULL;
}

void stable_test_multithreaded() {
    printf("---multithreaded---\n");
    const size_t num_threads = 8;
    pthread_t threads[num_threads];
    char th_ids[num_threads][16]; 
    for (size_t i = 0; i < num_threads; i++) {
        snprintf(th_ids[i], sizeof(th_ids[i]), "%zu", i);
        pthread_create(&threads[i], NULL, stable_multithreaded, (void*)th_ids[i]);
        usleep(100000);
    }
    for (size_t i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);
    
}

int main() {
    printf("[test] stable test\n");
    stable_test();
    printf("[test] multithreaded test\n");
    stable_test_multithreaded();
    return 0;
}
#endif