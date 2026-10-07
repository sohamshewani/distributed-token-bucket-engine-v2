#ifndef DTB_TYPES_H
#define DTB_TYPES_H

#include <stdatomic.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <time.h>

#define DTB_CACHE_LINE_SIZE 64
#define DTB_SLIDING_WINDOW_SLOTS 8
#define DTB_MAX_HAZARD_POINTERS 64
#define DTB_EPSILON_CLOCK_SKEW_NS 5000000ULL /* 5 ms */

typedef enum {
    DTB_OK = 0,
    DTB_ERR_INVALID_ARG = -1,
    DTB_ERR_NO_MEM = -2,
    DTB_ERR_CONCONTENTION = -3,
    DTB_ERR_BUCKET_EXHAUSTED = -4,
    DTB_ERR_CLOCK_SKEW = -5,
    DTB_ERR_PARTITION = -6
} dtb_status_t;

typedef struct {
    _Atomic uint64_t count;
    uint64_t timestamp_ns;
} dtb_micro_bucket_t;

/* Cache-line aligned token bucket structure (64 bytes) */
typedef struct {
    _Atomic uint64_t tokens;
    _Atomic uint64_t last_refill_ns;
    dtb_micro_bucket_t window[DTB_SLIDING_WINDOW_SLOTS];
    uint32_t window_head;
    uint32_t capacity;
    uint32_t refill_rate_per_sec;
    char pad[DTB_CACHE_LINE_SIZE - (sizeof(_Atomic uint64_t) * 2 + sizeof(dtb_micro_bucket_t) * DTB_SLIDING_WINDOW_SLOTS + sizeof(uint32_t) * 3)];
} dtb_bucket_t;

_Static_assert(sizeof(dtb_bucket_t) == DTB_CACHE_LINE_SIZE, "dtb_bucket_t must be exactly 64 bytes");

typedef struct {
    uint64_t sequence;
    uint32_t node_id;
    uint32_t event_type;
    int64_t payload[4];
} dtb_ring_event_t;

typedef struct {
    _Atomic uint64_t head;
    _Atomic uint64_t tail;
    uint32_t capacity;
    uint32_t mask;
    dtb_ring_event_t *buffer;
} dtb_ring_buffer_t;

typedef struct {
    uint8_t *base;
    uint8_t *ptr;
    size_t capacity;
    _Atomic size_t offset;
} dtb_arena_t;

typedef struct {
    uint32_t node_id;
    uint64_t physical_time_ns;
    uint64_t logical_counter;
    int64_t adaptive_offset_ns;
    bool is_active;
    char pad[DTB_CACHE_LINE_SIZE - (sizeof(uint32_t) + sizeof(uint64_t) * 3 + sizeof(int64_t) + sizeof(bool))];
} dtb_node_state_t;

_Static_assert(sizeof(dtb_node_state_t) == DTB_CACHE_LINE_SIZE, "dtb_node_state_t must be exactly 64 bytes");

typedef struct {
    _Atomic(void *) pointer;
} dtb_hazard_slot_t;

typedef struct {
    dtb_hazard_slot_t slots[DTB_MAX_HAZARD_POINTERS];
} dtb_hazard_domain_t;

typedef struct {
    uint64_t physical_ns;
    uint64_t logical;
    uint32_t node_id;
} dtb_hlc_t;

static inline void dtb_cpu_relax(void) {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__) || defined(_M_ARM64)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

static inline dtb_status_t dtb_ring_buffer_init(dtb_ring_buffer_t *rb, uint32_t capacity, dtb_arena_t *arena) {
    if (!rb || capacity == 0 || (capacity & (capacity - 1)) != 0 || !arena) {
        return DTB_ERR_INVALID_ARG;
    }
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    
    size_t alloc_size = sizeof(dtb_ring_event_t) * capacity;
    size_t current_offset = atomic_load(&arena->offset);
    if (current_offset + alloc_size > arena->capacity) {
        return DTB_ERR_NO_MEM;
    }
    
    rb->buffer = (dtb_ring_event_t *)(arena->base + current_offset);
    atomic_store(&arena->offset, current_offset + alloc_size);
    return DTB_OK;
}

static inline bool dtb_ring_buffer_push(dtb_ring_buffer_t *rb, const dtb_ring_event_t *event) {
    uint64_t current_head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    uint64_t current_tail;
    
    do {
        current_tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
        if ((current_head - current_tail) >= rb->capacity) {
            return false; /* Ring buffer is full */
        }
    } while (!atomic_compare_exchange_weak_explicit(&rb->head, &current_head, current_head + 1,
                                                    memory_order_release, memory_order_relaxed));
                                                    
    rb->buffer[current_head & rb->mask] = *event;
    
    while (atomic_load_explicit(&rb->tail, memory_order_relaxed) != current_head) {
        dtb_cpu_relax();
    }
    atomic_store_explicit(&rb->tail, current_head + 1, memory_order_release);
    return true;
}

static inline bool dtb_ring_buffer_pop(dtb_ring_buffer_t *rb, dtb_ring_event_t *event) {
    uint64_t current_tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    uint64_t current_head;
    
    do {
        current_head = atomic_load_explicit(&rb->head, memory_order_acquire);
        if (current_tail == current_head) {
            return false; /* Ring buffer is empty */
        }
        *event = rb->buffer[current_tail & rb->mask];
    } while (!atomic_compare_exchange_weak_explicit(&rb->tail, &current_tail, current_tail + 1,
                                                    memory_order_release, memory_order_relaxed));
    return true;
}

static inline dtb_status_t dtb_arena_init(dtb_arena_t *arena, void *memory, size_t capacity) {
    if (!arena || !memory || capacity == 0) {
        return DTB_ERR_INVALID_ARG;
    }
    arena->base = (uint8_t *)memory;
    arena->ptr = arena->base;
    arena->capacity = capacity;
    atomic_init(&arena->offset, 0);
    return DTB_OK;
}

static inline void *dtb_arena_alloc(dtb_arena_t *arena, size_t size, size_t alignment) {
    size_t current_offset = atomic_load(&arena->offset);
    size_t padding = (alignment - (current_offset % alignment)) % alignment;
    size_t total_size = size + padding;
    
    size_t new_offset;
    do {
        current_offset = atomic_load(&arena->offset);
        padding = (alignment - (current_offset % alignment)) % alignment;
        total_size = size + padding;
        if (current_offset + total_size > arena->capacity) {
            return NULL;
        }
        new_offset = current_offset + total_size;
    } while (!atomic_compare_exchange_weak(&arena->offset, &current_offset, new_offset));
    
    return (void *)(arena->base + current_offset + padding);
}

static inline void dtb_bucket_init(dtb_bucket_t *bucket, uint32_t capacity, uint32_t refill_rate) {
    atomic_init(&bucket->tokens, capacity);
    uint64_t now = (uint64_t)time(NULL) * 1000000000ULL;
    atomic_init(&bucket->last_refill_ns, now);
    bucket->capacity = capacity;
    bucket->refill_rate_per_sec = refill_rate;
    bucket->window_head = 0;
    for (int i = 0; i < DTB_SLIDING_WINDOW_SLOTS; ++i) {
        bucket->window[i].count = 0;
        bucket->window[i].timestamp_ns = 0;
    }
}

static inline dtb_status_t dtb_bucket_consume(dtb_bucket_t *bucket, uint64_t tokens_requested, uint64_t current_time_ns) {
    uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_acquire);
    uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
    
    if (current_time_ns > last_refill) {
        uint64_t delta_ns = current_time_ns - last_refill;
        uint64_t generated = (delta_ns * bucket->refill_rate_per_sec) / 1000000000ULL;
        if (generated > 0) {
            uint64_t expected_tokens = current_tokens;
            uint64_t new_tokens;
            do {
                new_tokens = (expected_tokens + generated > bucket->capacity) ? bucket->capacity : (expected_tokens + generated);
            } while (!atomic_compare_exchange_weak_explicit(&bucket->tokens, &expected_tokens, new_tokens,
                                                            memory_order_release, memory_order_relaxed));
            current_tokens = new_tokens;
            atomic_store_explicit(&bucket->last_refill_ns, current_time_ns, memory_order_relaxed);
        }
    }

    uint32_t backoff = 0;
    while (true) {
        if (current_tokens < tokens_requested) {
            return DTB_ERR_BUCKET_EXHAUSTED;
        }
        
        if (atomic_compare_exchange_weak_explicit(&bucket->tokens, &current_tokens, current_tokens - tokens_requested,
                                                  memory_order_acq_rel, memory_order_acquire)) {
            uint32_t slot = bucket->window_head % DTB_SLIDING_WINDOW_SLOTS;
            atomic_store_explicit(&bucket->window[slot].count, tokens_requested, memory_order_relaxed);
            bucket->window[slot].timestamp_ns = current_time_ns;
            bucket->window_head++;
            return DTB_OK;
        }
        
        for (uint32_t i = 0; i < (1U << backoff); ++i) {
            dtb_cpu_relax();
        }
        if (backoff < 8) {
            backoff++;
        }
    }
}

static inline void dtb_hlc_update(dtb_hlc_t *local, uint64_t remote_physical_ns, uint64_t remote_logical, int64_t *adaptive_offset_ns) {
    uint64_t local_physical = (uint64_t)time(NULL) * 1000000000ULL;
    int64_t skew = (int64_t)remote_physical_ns - (int64_t)local_physical;
    
    *adaptive_offset_ns = (*adaptive_offset_ns * 7 + skew) / 8;
    
    if (skew > DTB_EPSILON_CLOCK_SKEW_NS || skew < -DTB_EPSILON_CLOCK_SKEW_NS) {
        /* Correction factor applied via dampening */
        local->physical_ns = local_physical + *adaptive_offset_ns;
        local->logical = (local->logical > remote_logical ? local->logical : remote_logical) + 1;
    } else {
        if (local_physical > local->physical_ns) {
            local->physical_ns = local_physical;
            local->logical = 0;
        } else if (local_physical == local->physical_ns) {
            if (remote_logical > local->logical) {
                local->logical = remote_logical + 1;
            } else {
                local->logical++;
            }
        } else {
            if (remote_logical > local->logical) {
                local->logical = remote_logical + 1;
            } else {
                local->logical++;
            }
        }
    }
}

#endif /* DTB_TYPES_H */