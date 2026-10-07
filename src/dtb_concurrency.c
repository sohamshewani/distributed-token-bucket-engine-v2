#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <immintrin.h>

#define DTB_CACHE_LINE 64
#define DTB_SLIDING_WINDOWS 8
#define DTB_MAX_HAZARDS_PER_THREAD 8
#define DTB_MAX_THREADS 128
#define DTB_EPSILON_NS 5000000ULL /* 5ms */

typedef struct dtb_micro_bucket {
    _Atomic uint64_t tokens;
    _Atomic uint64_t timestamp;
} dtb_micro_bucket_t;

typedef struct dtb_bucket {
    _Atomic uint64_t tokens;
    _Atomic uint64_t last_refill_ns;
    uint64_t capacity;
    uint64_t refill_rate_per_sec;
    dtb_micro_bucket_t windows[DTB_SLIDING_WINDOWS];
    uint8_t padding[DTB_CACHE_LINE - ((sizeof(_Atomic uint64_t) * 2 + sizeof(uint64_t) * 2 + sizeof(dtb_micro_bucket_t) * DTB_SLIDING_WINDOWS) % DTB_CACHE_LINE)];
} __attribute__((aligned(DTB_CACHE_LINE))) dtb_bucket_t;

typedef struct dtb_ring_node {
    _Atomic uint64_t sequence;
    uint32_t event_type;
    int64_t payload_a;
    int64_t payload_b;
} dtb_ring_node_t;

typedef struct dtb_ring_buffer {
    size_t capacity;
    size_t mask;
    char pad1[DTB_CACHE_LINE - (sizeof(size_t) * 2 % DTB_CACHE_LINE)];
    _Atomic uint64_t head;
    char pad2[DTB_CACHE_LINE - (sizeof(_Atomic uint64_t) % DTB_CACHE_LINE)];
    _Atomic uint64_t tail;
    char pad3[DTB_CACHE_LINE - (sizeof(_Atomic uint64_t) % DTB_CACHE_LINE)];
    dtb_ring_node_t *buffer;
} __attribute__((aligned(DTB_CACHE_LINE))) dtb_ring_buffer_t;

typedef struct dtb_arena {
    uint8_t *memory;
    size_t capacity;
    _Atomic size_t offset;
} dtb_arena_t;

typedef struct dtb_node_state {
    uint64_t node_id;
    _Atomic uint64_t hlc_physical;
    _Atomic uint64_t hlc_logical;
    _Atomic int64_t clock_offset_ns;
    _Atomic bool partitioned;
    uint32_t weight_proportion;
} dtb_node_state_t;

typedef struct dtb_hazard {
    _Atomic(void *) ptr;
} dtb_hazard_t;

typedef struct dtb_hazard_domain {
    dtb_hazard_t hazards[DTB_MAX_THREADS][DTB_MAX_HAZARDS_PER_THREAD];
} dtb_hazard_domain_t;

static dtb_hazard_domain_t g_hazard_domain;
static _Atomic size_t g_active_thread_id = 0;

static inline size_t dtb_get_thread_id(void) {
    static _Thread_local int tid = -1;
    if (tid == -1) {
        tid = (int)(atomic_fetch_add(&g_active_thread_id, 1) % DTB_MAX_THREADS);
    }
    return (size_t)tid;
}

void dtb_hazard_set(size_t hazard_index, void *ptr) {
    size_t tid = dtb_get_thread_id();
    atomic_store_explicit(&g_hazard_domain.hazards[tid][hazard_index].ptr, ptr, memory_order_seq_cst);
}

void dtb_hazard_clear(size_t hazard_index) {
    size_t tid = dtb_get_thread_id();
    atomic_store_explicit(&g_hazard_domain.hazards[tid][hazard_index].ptr, NULL, memory_order_release);
}

void *dtb_hazard_get(size_t hazard_index) {
    size_t tid = dtb_get_thread_id();
    return atomic_load_explicit(&g_hazard_domain.hazards[tid][hazard_index].ptr, memory_order_acquire);
}

bool dtb_hazard_is_protected(void *ptr) {
    for (size_t i = 0; i < DTB_MAX_THREADS; ++i) {
        for (size_t j = 0; j < DTB_MAX_HAZARDS_PER_THREAD; ++j) {
            if (atomic_load_explicit(&g_hazard_domain.hazards[i][j].ptr, memory_order_acquire) == ptr) {
                return true;
            }
        }
    }
    return false;
}

dtb_arena_t *dtb_arena_create(size_t capacity) {
    dtb_arena_t *arena = (dtb_arena_t *)malloc(sizeof(dtb_arena_t));
    if (!arena) return NULL;
    arena->memory = (uint8_t *)aligned_alloc(DTB_CACHE_LINE, capacity);
    if (!arena->memory) {
        free(arena);
        return NULL;
    }
    arena->capacity = capacity;
    atomic_init(&arena->offset, 0);
    return arena;
}

void dtb_arena_destroy(dtb_arena_t *arena) {
    if (arena) {
        free(arena->memory);
        free(arena);
    }
}

void *dtb_arena_alloc(dtb_arena_t *arena, size_t size) {
    size_t aligned_size = (size + (DTB_CACHE_LINE - 1)) & ~(DTB_CACHE_LINE - 1);
    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (current_offset + aligned_size > arena->capacity) {
            return NULL;
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current_offset, current_offset + aligned_size,
                                                 memory_order_relaxed, memory_order_relaxed)) {
            return (void *)(arena->memory + current_offset);
        }
    }
}

void dtb_arena_reset(dtb_arena_t *arena) {
    atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
}

dtb_ring_buffer_t *dtb_ring_buffer_create(size_t capacity_pow2) {
    dtb_ring_buffer_t *rb = (dtb_ring_buffer_t *)malloc(sizeof(dtb_ring_buffer_t));
    if (!rb) return NULL;
    
    rb->capacity = capacity_pow2;
    rb->mask = capacity_pow2 - 1;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    
    rb->buffer = (dtb_ring_node_t *)aligned_alloc(DTB_CACHE_LINE, sizeof(dtb_ring_node_t) * capacity_pow2);
    if (!rb->buffer) {
        free(rb);
        return NULL;
    }
    
    for (size_t i = 0; i < capacity_pow2; ++i) {
        atomic_init(&rb->buffer[i].sequence, i);
        rb->buffer[i].event_type = 0;
        rb->buffer[i].payload_a = 0;
        rb->buffer[i].payload_b = 0;
    }
    
    return rb;
}

void dtb_ring_buffer_destroy(dtb_ring_buffer_t *rb) {
    if (rb) {
        free(rb->buffer);
        free(rb);
    }
}

bool dtb_ring_buffer_push(dtb_ring_buffer_t *rb, uint32_t event_type, int64_t a, int64_t b) {
    dtb_ring_node_t *node;
    uint64_t pos = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    
    while (1) {
        node = &rb->buffer[pos & rb->mask];
        uint64_t seq = atomic_load_explicit(&node->sequence, memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&rb->tail, &pos, pos + 1,
                                                     memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            pos = atomic_load_explicit(&rb->tail, memory_order_relaxed);
        }
    }
    
    node->event_type = event_type;
    node->payload_a = a;
    node->payload_b = b;
    atomic_store_explicit(&node->sequence, pos + 1, memory_order_release);
    return true;
}

bool dtb_ring_buffer_pop(dtb_ring_buffer_t *rb, uint32_t *event_type, int64_t *a, int64_t *b) {
    dtb_ring_node_t *node;
    uint64_t pos = atomic_load_explicit(&rb->head, memory_order_relaxed);
    
    while (1) {
        node = &rb->buffer[pos & rb->mask];
        uint64_t seq = atomic_load_explicit(&node->sequence, memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        
        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(&rb->head, &pos, pos + 1,
                                                     memory_order_relaxed, memory_order_relaxed)) {
                break;
            }
        } else if (diff < 0) {
            return false;
        } else {
            pos = atomic_load_explicit(&rb->head, memory_order_relaxed);
        }
    }
    
    *event_type = node->event_type;
    *a = node->payload_a;
    *b = node->payload_b;
    atomic_store_explicit(&node->sequence, pos + rb->mask + 1, memory_order_release);
    return true;
}

static uint64_t dtb_get_monotonic_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

void dtb_bucket_init(dtb_bucket_t *bucket, uint64_t capacity, uint64_t refill_rate_per_sec) {
    atomic_init(&bucket->tokens, capacity);
    uint64_t now = dtb_get_monotonic_ns();
    atomic_init(&bucket->last_refill_ns, now);
    bucket->capacity = capacity;
    bucket->refill_rate_per_sec = refill_rate_per_sec;
    
    for (int i = 0; i < DTB_SLIDING_WINDOWS; ++i) {
        atomic_init(&bucket->windows[i].tokens, 0);
        atomic_init(&bucket->windows[i].timestamp, now);
    }
}

bool dtb_bucket_consume(dtb_bucket_t *bucket, uint64_t requested_tokens, dtb_node_state_t *node_state) {
    uint64_t backoff_iters = 0;
    uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_acquire);
    
    while (1) {
        uint64_t now = dtb_get_monotonic_ns();
        int64_t offset = atomic_load_explicit(&node_state->clock_offset_ns, memory_order_relaxed);
        uint64_t adjusted_now = (now > (uint64_t)(-offset)) ? (now + offset) : 0;
        
        uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
        
        if (adjusted_now > last_refill) {
            uint64_t delta_ns = adjusted_now - last_refill;
            uint64_t generated = (delta_ns * bucket->refill_rate_per_sec) / 1000000000ULL;
            
            if (generated > 0) {
                if (atomic_compare_exchange_weak_explicit(&bucket->last_refill_ns, &last_refill, adjusted_now,
                                                         memory_order_relaxed, memory_order_relaxed)) {
                    uint64_t old_tokens = atomic_load_explicit(&bucket->tokens, memory_order_relaxed);
                    uint64_t new_tokens;
                    do {
                        new_tokens = old_tokens + generated;
                        if (new_tokens > bucket->capacity) new_tokens = bucket->capacity;
                    } while (!atomic_compare_exchange_weak_explicit(&bucket->tokens, &old_tokens, new_tokens,
                                                                   memory_order_release, memory_order_relaxed));
                    current_tokens = new_tokens;
                }
            }
        }
        
        bool is_partitioned = atomic_load_explicit(&node_state->partitioned, memory_order_acquire);
        uint64_t effective_requested = requested_tokens;
        if (is_partitioned) {
            effective_requested = (requested_tokens * node_state->weight_proportion) / 100;
            if (effective_requested == 0) effective_requested = 1;
        }
        
        current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_relaxed);
        if (current_tokens < effective_requested) {
            return false;
        }
        
        if (atomic_compare_exchange_weak_explicit(&bucket->tokens, &current_tokens, current_tokens - effective_requested,
                                                 memory_order_seq_cst, memory_order_acquire)) {
            int win_idx = (adjusted_now / (1000000000ULL / DTB_SLIDING_WINDOWS)) % DTB_SLIDING_WINDOWS;
            atomic_fetch_add_explicit(&bucket->windows[win_idx].tokens, effective_requested, memory_order_relaxed);
            atomic_store_explicit(&bucket->windows[win_idx].timestamp, adjusted_now, memory_order_relaxed);
            return true;
        }
        
        for (uint32_t i = 0; i < (1U << backoff_iters); ++i) {
            _mm_pause();
        }
        if (backoff_iters < 16) {
            backoff_iters++;
        }
    }
}

void dtb_hlc_update(dtb_node_state_t *state, uint64_t remote_physical, uint64_t remote_logical) {
    uint64_t local_physical = dtb_get_monotonic_ns();
    uint64_t current_physical = atomic_load_explicit(&state->hlc_physical, memory_order_relaxed);
    uint64_t current_logical = atomic_load_explicit(&state->hlc_logical, memory_order_relaxed);
    
    uint64_t max_physical = current_physical;
    if (local_physical > max_physical) max_physical = local_physical;