#define _GNU_SOURCE
#include <stdint.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <immintrin.h>
#include <time.h>
#include <errno.h>

#define DTB_CACHE_LINE_SIZE 64
#define DTB_RING_BUFFER_ALIGN DTB_CACHE_LINE_SIZE

typedef struct __attribute__((aligned(DTB_CACHE_LINE_SIZE))) {
    _Atomic uint64_t tokens;
    _Atomic uint64_t last_refill_ns;
    uint64_t window_micro_buckets[4];
    uint8_t pad[DTB_CACHE_LINE_SIZE - (sizeof(_Atomic uint64_t) * 2 + sizeof(uint64_t) * 4)];
} dtb_bucket_t;

typedef struct {
    uint64_t event_id;
    uint64_t timestamp_ns;
    uint32_t node_id;
    int32_t delta_tokens;
    uint32_t flags;
} dtb_ring_event_t;

typedef struct __attribute__((aligned(DTB_RING_BUFFER_ALIGN))) {
    _Atomic uint64_t head;
    _Atomic uint64_t tail;
    uint64_t capacity_mask;
    dtb_ring_event_t *events;
} dtb_ring_buffer_t;

static inline void dtb_cpu_relax(void) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    atomic_signal_fence(memory_order_seq_cst);
#endif
}

bool dtb_ring_buffer_init(dtb_ring_buffer_t *rb, size_t capacity_power_of_two) {
    if (!rb || (capacity_power_of_two == 0) || (capacity_power_of_two & (capacity_power_of_two - 1))) {
        errno = EINVAL;
        return false;
    }

    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    rb->capacity_mask = capacity_power_of_two - 1;

    int ret = posix_memalign((void **)&rb->events, DTB_RING_BUFFER_ALIGN, sizeof(dtb_ring_event_t) * capacity_power_of_two);
    if (ret != 0 || !rb->events) {
        errno = ENOMEM;
        return false;
    }

    memset(rb->events, 0, sizeof(dtb_ring_event_t) * capacity_power_of_two);
    return true;
}

void dtb_ring_buffer_free(dtb_ring_buffer_t *rb) {
    if (rb) {
        free(rb->events);
        rb->events = NULL;
        rb->capacity_mask = 0;
    }
}

bool dtb_ring_buffer_push(dtb_ring_buffer_t *rb, const dtb_ring_event_t *event) {
    if (!rb || !event) {
        errno = EINVAL;
        return false;
    }

    uint64_t current_head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    
    for (;;) {
        uint64_t current_tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
        
        if ((current_head - current_tail) > rb->capacity_mask) {
            return false;
        }

        if (atomic_compare_exchange_weak_explicit(&rb->head, &current_head, current_head + 1,
                                                memory_order_release, memory_order_relaxed)) {
            uint64_t index = current_head & rb->capacity_mask;
            
            memcpy(&rb->events[index], event, sizeof(dtb_ring_event_t));
            
            while (atomic_load_explicit(&rb->tail, memory_order_relaxed) != current_head) {
                dtb_cpu_relax();
            }
            
            atomic_store_explicit(&rb->tail, current_head + 1, memory_order_release);
            return true;
        }
        dtb_cpu_relax();
    }
}

bool dtb_ring_buffer_pop(dtb_ring_buffer_t *rb, dtb_ring_event_t *event) {
    if (!rb || !event) {
        errno = EINVAL;
        return false;
    }

    uint64_t current_tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    
    for (;;) {
        uint64_t current_head = atomic_load_explicit(&rb->head, memory_order_acquire);
        
        if (current_tail == current_head) {
            return false;
        }

        uint64_t index = current_tail & rb->capacity_mask;
        memcpy(event, &rb->events[index], sizeof(dtb_ring_event_t));

        if (atomic_compare_exchange_weak_explicit(&rb->tail, &current_tail, current_tail + 1,
                                                memory_order_release, memory_order_relaxed)) {
            return true;
        }
        dtb_cpu_relax();
    }
}

size_t dtb_ring_buffer_size(const dtb_ring_buffer_t *rb) {
    if (!rb) return 0;
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    if (head >= tail) {
        return (size_t)(head - tail);
    }
    return 0;
}

bool dtb_bucket_consume(dtb_bucket_t *bucket, uint64_t tokens_requested, uint64_t capacity, uint64_t refill_rate_per_ns, uint64_t current_time_ns) {
    if (!bucket || tokens_requested == 0) {
        return false;
    }

    uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
    
    for (;;) {
        if (current_time_ns < last_refill) {
            current_time_ns = last_refill;
        }

        uint64_t elapsed = current_time_ns - last_refill;
        uint64_t generated = elapsed * refill_rate_per_ns;
        
        uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_acquire);
        uint64_t new_tokens = current_tokens + generated;
        if (new_tokens > capacity) {
            new_tokens = capacity;
        }

        if (new_tokens < tokens_requested) {
            return false;
        }

        uint64_t final_tokens = new_tokens - tokens_requested;

        if (atomic_compare_exchange_weak_explicit(&bucket->tokens, &current_tokens, final_tokens,
                                                memory_order_release, memory_order_relaxed)) {
            uint64_t expected_time = last_refill;
            atomic_compare_exchange_strong_explicit(&bucket->last_refill_ns, &expected_time, current_time_ns,
                                                  memory_order_relaxed, memory_order_relaxed);
            return true;
        }

        dtb_cpu_relax();
        last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
    }
}