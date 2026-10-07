#include "dtb_sync.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif

static inline void dtb_cpu_relax(void) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    /* Generic fallback */
    __asm__ __volatile__("" ::: "memory");
#endif
}

static uint64_t dtb_get_monotonic_time_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

int dtb_arena_init(dtb_arena_t *arena, size_t capacity) {
    if (!arena || capacity == 0) {
        return -1;
    }
    arena->base = malloc(capacity);
    if (!arena->base) {
        return -1;
    }
    arena->capacity = capacity;
    atomic_init(&arena->offset, 0);
    return 0;
}

void dtb_arena_destroy(dtb_arena_t *arena) {
    if (!arena) {
        return;
    }
    free(arena->base);
    arena->base = NULL;
    arena->capacity = 0;
    atomic_init(&arena->offset, 0);
}

void *dtb_arena_alloc(dtb_arena_t *arena, size_t size, size_t alignment) {
    if (!arena || size == 0) {
        return NULL;
    }
    
    /* Ensure alignment is a power of two */
    if ((alignment & (alignment - 1)) != 0 || alignment == 0) {
        alignment = 8;
    }

    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        size_t aligned_offset = (current_offset + alignment - 1) & ~(alignment - 1);
        size_t next_offset = aligned_offset + size;
        
        if (next_offset > arena->capacity) {
            return NULL; /* Out of memory in arena */
        }
        
        if (atomic_compare_exchange_weak_explicit(&arena->offset,
                                                  &current_offset,
                                                  next_offset,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            return (void *)((uintptr_t)arena->base + aligned_offset);
        }
    }
}

int dtb_ring_init(dtb_ring_buffer_t *ring, size_t capacity_pow2) {
    if (!ring || capacity_pow2 == 0) {
        return -1;
    }
    
    /* Verify capacity is power of two */
    if ((capacity_pow2 & (capacity_pow2 - 1)) != 0) {
        return -1;
    }
    
    ring->buffer = calloc(capacity_pow2, sizeof(dtb_telemetry_event_t));
    if (!ring->buffer) {
        return -1;
    }
    
    ring->capacity = capacity_pow2;
    ring->mask = capacity_pow2 - 1;
    atomic_init(&ring->head, 0);
    atomic_init(&ring->tail, 0);
    return 0;
}

void dtb_ring_destroy(dtb_ring_buffer_t *ring) {
    if (!ring) {
        return;
    }
    free(ring->buffer);
    ring->buffer = NULL;
    ring->capacity = 0;
    ring->mask = 0;
}

bool dtb_ring_push(dtb_ring_buffer_t *ring, const dtb_telemetry_event_t *event) {
    uint64_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint64_t tail;
    
    while (1) {
        tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
        if (head - tail >= ring->capacity) {
            return false; /* Ring buffer is full */
        }
        
        if (atomic_compare_exchange_weak_explicit(&ring->head,
                                                  &head,
                                                  head + 1,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            break;
        }
    }
    
    ring->buffer[head & ring->mask] = *event;
    
    /* Advance tail to signal item is fully written */
    while (!atomic_compare_exchange_weak_explicit(&ring->tail,
                                                  &head,
                                                  head + 1,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
        dtb_cpu_relax();
        head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    }
    
    return true;
}

bool dtb_ring_pop(dtb_ring_buffer_t *ring, dtb_telemetry_event_t *event) {
    uint64_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint64_t head;
    
    while (1) {
        head = atomic_load_explicit(&ring->head, memory_order_acquire);
        if (tail >= head) {
            return false; /* Ring buffer is empty */
        }
        
        *event = ring->buffer[tail & ring->mask];
        
        if (atomic_compare_exchange_weak_explicit(&ring->tail,
                                                  &tail,
                                                  tail + 1,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            return true;
        }
    }
}

void dtb_bucket_init(dtb_bucket_t *bucket, uint64_t capacity, uint64_t refill_rate) {
    if (!bucket) {
        return;
    }
    memset(bucket, 0, sizeof(dtb_bucket_t));
    atomic_init(&bucket->tokens, capacity);
    atomic_init(&bucket->last_refill_ns, dtb_get_monotonic_time_ns());
    bucket->capacity = capacity;
    bucket->refill_rate_per_ns = refill_rate;
    atomic_init(&bucket->correction_factor, 1000); /* 1.000 scaled by 1000 */
    
    for (int i = 0; i < DTB_WINDOW_BUCKETS; i++) {
        atomic_init(&bucket->window[i], 0);
    }
}

bool dtb_bucket_consume(dtb_bucket_t *bucket, uint64_t tokens_requested, uint64_t *out_remaining) {
    if (!bucket || tokens_requested == 0) {
        return false;
    }
    
    uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_acquire);
    uint64_t now = dtb_get_monotonic_time_ns();
    uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_acquire);
    
    unsigned int backoff_attempts = 0;
    
    while (1) {
        /* Calculate refill */
        if (now > last_refill) {
            uint64_t delta_ns = now - last_refill;
            uint32_t correction = atomic_load_explicit(&bucket->correction_factor, memory_order_relaxed);
            
            /* Apply correction factor for clock skew dampening */
            __uint128_t generated = (__uint128_t)delta_ns * bucket->refill_rate_per_ns * correction / 1000ULL;
            __uint128_t new_tokens = (__uint128_t)current_tokens + generated;
            
            if (new_tokens > bucket->capacity) {
                current_tokens = bucket->capacity;
            } else {
                current_tokens = (uint64_t)new_tokens;
            }
        }
        
        if (current_tokens < tokens_requested) {
            if (out_remaining) {
                *out_remaining = current_tokens;
            }
            return false; /* Insufficient tokens */
        }
        
        uint64_t next_tokens = current_tokens - tokens_requested;
        
        if (atomic_compare_exchange_weak_explicit(&bucket->tokens,
                                                  &current_tokens,
                                                  next_tokens,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
            /* Successfully consumed tokens. Update refill timestamp if needed. */
            atomic_store_explicit(&bucket->last_refill_ns, now, memory_order_release);
            
            /* Update sliding window micro-bucket */
            int window_idx = (int)((now / (DTB_WINDOW_DURATION_MS * 1000000ULL)) % DTB_WINDOW_BUCKETS);
            atomic_fetch_add_explicit(&bucket->window[window_idx], tokens_requested, memory_order_relaxed);
            
            if (out_remaining) {
                *out_remaining = next_tokens;
            }
            return true;
        }
        
        dtb_cpu_relax();
        backoff_attempts++;
        if (backoff_attempts > 64) {
            now = dtb_get_monotonic_time_ns();
            backoff_attempts = 0;
        }
    }
}

void dtb_hlc_tick(dtb_hlc_t *hlc, uint64_t physical_time_ns) {
    if (!hlc) {
        return;
    }
    
    uint64_t current_l = atomic_load_explicit(&hlc->logical_time, memory_order_acquire);
    uint64_t pt = (current_l >> 16);
    uint64_t c = current_l & 0xFFFF;
    
    uint64_t new_pt = physical_time_ns > pt ? physical_time_ns : pt;
    uint64_t new_c = 0;
    
    if (new_pt == pt) {
        new_c = c + 1;
    } else {
        new_c = 0;
    }
    
    uint64_t next_l = (new_pt << 16) | (new_c & 0xFFFF);
    
    while (!atomic_compare_exchange_weak_explicit(&hlc->logical_time,
                                                  &current_l,
                                                  next_l,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
        pt = (current_l >> 16);
        c = current_l & 0xFFFF;
        new_pt = physical_time_ns > pt ? physical_time_ns : pt;
        if (new_pt == pt) {
            new_c = c + 1;
        } else {
            new_c = 0;
        }
        next_l = (new_pt << 16) | (new_c & 0xFFFF);
        dtb_cpu_relax();
    }
}

void dtb_hlc_update(dtb_hlc_t *hlc, uint64_t remote_logical_time, uint64_t physical_time_ns) {
    if (!hlc) {
        return;
    }
    
    uint64_t remote_pt = (remote_logical_time >> 16);
    uint64_t remote_c = remote_logical_time & 0xFFFF;
    
    uint64_t current_l = atomic_load_explicit(&hlc->logical_time, memory_order_acquire);
    uint64_t pt = (current_l >> 16);
    uint64_t c = current_l & 0xFFFF;
    
    uint64_t max_pt = pt;
    if (physical_time_ns > max_pt) max_pt = physical_time_ns;
    if (remote_pt > max_pt) max_pt = remote_pt;
    
    uint64_t new_c = 0;
    if (max_pt == pt && max_pt == remote_pt) {
        new_c = (c > remote_c ? c : remote_c) + 1;
    } else if (max_pt == pt) {
        new_c = c + 1;
    } else if (max_pt == remote_pt) {
        new_c = remote_c + 1;
    } else {
        new_c = 0;
    }
    
    uint64_t next_l = (max_pt << 16) | (new_c & 0xFFFF);
    
    while (!atomic_compare_exchange_weak_explicit(&hlc->logical_time,
                                                  &current_l,
                                                  next_l,
                                                  memory_order_acq_rel,
                                                  memory_order_acquire)) {
        pt = (current_l >> 16);
        c = current_l & 0xFFFF;
        max_pt = pt;
        if (physical_time_ns > max_pt) max_pt = physical_time_ns;
        if (remote_pt > max_pt) max_pt = remote_pt;
        
        if (max_pt == pt && max_pt == remote_pt) {
            new_c = (c > remote_c ? c : remote_c) + 1;
        } else if (max_pt == pt) {
            new_c = c + 1;
        } else if (max_pt == remote_pt) {
            new_c = remote_c + 1;
        } else {
            new_c = 0;
        }
        next_l = (max_pt << 16) | (new_c & 0xFFFF);
        dtb_cpu_relax();
    }
}

void dtb_estimator_process(dtb_clock_estimator_t *est, uint64_t remote_physical_ns, uint64_t local_physical_ns) {
    if (!est) {
        return;
    }
    
    int64_t raw_offset;
    if (remote_physical_ns >= local_physical_ns) {
        raw_offset = (int64_t)(remote_physical_ns - local_physical_ns);
    } else {
        raw_offset = -((int64_t)(local_physical_ns - remote_physical_ns));
    }
    
    /* Exponential Moving Average (EMA) for offset estimation */
    const double alpha = 0.125;
    est->estimated_offset_ns = (int64_t)((1.0 - alpha) * (double)est->estimated_offset_ns + alpha * (double)raw_offset);
    
    /* Check for excessive skew: epsilon threshold > 5ms (5,000,000 ns) */
    int64_t abs_offset = est->estimated_offset_ns >= 0 ? est->estimated_offset_ns : -est->estimated_offset_ns;
    if (abs_offset > 5000000LL) {
        est->skew_detected = true;
    } else {
        est->skew_detected = false;
    }
}

void dtb_cluster_apply_skew_dampening(dtb_bucket_t *bucket, const dtb_clock_estimator_t *est) {
    if (!bucket || !est) {
        return;
    }
    
    if (est->skew_detected) {
        /* Dampen refill correction factor by 20% to bound over-allocation under skew */
        uint32_t current_correction = atomic_load_explicit(&bucket->correction_factor, memory_order_relaxed);
        uint32_t dampened = current