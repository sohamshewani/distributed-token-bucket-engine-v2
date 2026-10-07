#include "dtb_engine.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <immintrin.h>

#define CACHE_LINE_SIZE 64

static inline uint64_t dtb_get_monotonic_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline void dtb_cpu_relax(void) {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    /* generic fallback */
#endif
}

dtb_arena_t* dtb_arena_create(size_t total_size) {
    size_t aligned_size = (total_size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
    size_t header_size = (sizeof(dtb_arena_t) + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
    
    char* raw = (char*)aligned_alloc(CACHE_LINE_SIZE, header_size + aligned_size);
    if (!raw) return NULL;
    
    dtb_arena_t* arena = (dtb_arena_t*)raw;
    arena->base = raw + header_size;
    arena->capacity = aligned_size;
    atomic_init(&arena->offset, 0);
    return arena;
}

void dtb_arena_destroy(dtb_arena_t* arena) {
    if (arena) {
        free(arena);
    }
}

void* dtb_arena_alloc(dtb_arena_t* arena, size_t size) {
    if (!arena) return NULL;
    size_t aligned_size = (size + 7) & ~7; /* 8-byte alignment */
    size_t current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
    
    while (1) {
        if (current_offset + aligned_size > arena->capacity) {
            return NULL; /* Out of arena memory */
        }
        if (atomic_compare_exchange_weak_explicit(&arena->offset, &current_offset,
            current_offset + aligned_size, memory_order_relaxed, memory_order_relaxed)) {
            return arena->base + current_offset;
        }
    }
}

void dtb_arena_reset(dtb_arena_t* arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_relaxed);
    }
}

dtb_ring_buffer_t* dtb_ring_buffer_create(size_t capacity, dtb_arena_t* arena) {
    /* capacity must be power of two */
    if (capacity == 0 || (capacity & (capacity - 1)) != 0) return NULL;
    
    dtb_ring_buffer_t* rb = (dtb_ring_buffer_t*)dtb_arena_alloc(arena, sizeof(dtb_ring_buffer_t));
    if (!rb) return NULL;
    
    rb->events = (dtb_telemetry_event_t*)dtb_arena_alloc(arena, sizeof(dtb_telemetry_event_t) * capacity);
    if (!rb->events) return NULL;
    
    rb->capacity = capacity;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    return rb;
}

bool dtb_ring_buffer_push(dtb_ring_buffer_t* rb, const dtb_telemetry_event_t* event) {
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
    
    while (1) {
        if (head - tail >= rb->capacity) {
            return false; /* Ring buffer full */
        }
        if (atomic_compare_exchange_weak_explicit(&rb->head, &head, head + 1,
            memory_order_release, memory_order_relaxed)) {
            break;
        }
    }
    
    size_t index = head & (rb->capacity - 1);
    rb->events[index] = *event;
    
    /* Ensure visibility of the event write before advancing tail */
    uint64_t expected_tail = head;
    while (!atomic_compare_exchange_weak_explicit(&rb->tail, &expected_tail, head + 1,
        memory_order_release, memory_order_relaxed)) {
        expected_tail = head;
        dtb_cpu_relax();
    }
    
    return true;
}

bool dtb_ring_buffer_pop(dtb_ring_buffer_t* rb, dtb_telemetry_event_t* event) {
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_acquire);
    
    while (1) {
        if (tail >= head) {
            return false; /* Ring buffer empty */
        }
        
        size_t index = tail & (rb->capacity - 1);
        *event = rb->events[index];
        
        if (atomic_compare_exchange_weak_explicit(&rb->tail, &tail, tail + 1,
            memory_order_release, memory_order_relaxed)) {
            return true;
        }
    }
}

dtb_bucket_t* dtb_bucket_create(uint64_t capacity, uint64_t refill_rate, dtb_arena_t* arena) {
    dtb_bucket_t* bucket = (dtb_bucket_t*)dtb_arena_alloc(arena, sizeof(dtb_bucket_t));
    if (!bucket) return NULL;
    
    memset(bucket, 0, sizeof(dtb_bucket_t));
    atomic_init(&bucket->tokens, capacity);
    atomic_init(&bucket->last_refill_ts, dtb_get_monotonic_time_ns());
    bucket->capacity = capacity;
    bucket->refill_rate_per_ns = (double)refill_rate / 1000000000.0;
    
    for (int i = 0; i < DTB_WINDOW_BUCKETS; i++) {
        atomic_init(&bucket->window[i], 0);
    }
    
    atomic_init(&bucket->correction_factor, 1000000); /* 1.0 in fixed-point (scale 10^6) */
    return bucket;
}

bool dtb_bucket_consume(dtb_bucket_t* bucket, uint64_t tokens_requested, dtb_engine_t* engine) {
    if (tokens_requested == 0) return true;
    
    uint64_t now = dtb_get_monotonic_time_ns();
    uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ts, memory_order_relaxed);
    
    /* Refill loop */
    while (now > last_refill) {
        if (atomic_compare_exchange_weak_explicit(&bucket->last_refill_ts, &last_refill, now,
            memory_order_relaxed, memory_order_relaxed)) {
            uint64_t delta_ns = now - last_refill;
            double raw_tokens_to_add = (double)delta_ns * bucket->refill_rate_per_ns;
            
            uint64_t correction = atomic_load_explicit(&bucket->correction_factor, memory_order_relaxed);
            double corrected_tokens = raw_tokens_to_add * ((double)correction / 1000000.0);
            
            uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_relaxed);
            while (1) {
                uint64_t new_tokens = current_tokens + (uint64_t)corrected_tokens;
                if (new_tokens > bucket->capacity) {
                    new_tokens = bucket->capacity;
                }
                if (atomic_compare_exchange_weak_explicit(&bucket->tokens, &current_tokens, new_tokens,
                    memory_order_relaxed, memory_order_relaxed)) {
                    break;
                }
            }
            break;
        }
        last_refill = atomic_load_explicit(&bucket->last_refill_ts, memory_order_relaxed);
    }
    
    /* Consumption loop with backoff */
    uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_acquire);
    int backoff_iters = 0;
    
    while (1) {
        if (current_tokens < tokens_requested) {
            if (engine && engine->telemetry_ring) {
                dtb_telemetry_event_t ev = {
                    .timestamp_ns = now,
                    .event_type = 1, /* Denied */
                    .key_hash = (uint64_t)(uintptr_t)bucket,
                    .tokens_delta = tokens_requested
                };
                dtb_ring_buffer_push(engine->telemetry_ring, &ev);
            }
            return false;
        }
        
        if (atomic_compare_exchange_weak_explicit(&bucket->tokens, &current_tokens,
            current_tokens - tokens_requested, memory_order_release, memory_order_relaxed)) {
            
            /* Update sliding window micro-bucket */
            int window_idx = (int)((now / 100000000ULL) % DTB_WINDOW_BUCKETS);
            atomic_fetch_add_explicit(&bucket->window[window_idx], tokens_requested, memory_order_relaxed);
            
            if (engine && engine->telemetry_ring) {
                dtb_telemetry_event_t ev = {
                    .timestamp_ns = now,
                    .event_type = 0, /* Allowed */
                    .key_hash = (uint64_t)(uintptr_t)bucket,
                    .tokens_delta = tokens_requested
                };
                dtb_ring_buffer_push(engine->telemetry_ring, &ev);
            }
            return true;
        }
        
        backoff_iters++;
        if (backoff_iters > 16) {
            dtb_cpu_relax();
        }
    }
}

dtb_node_state_t* dtb_node_state_create(uint64_t node_id, dtb_arena_t* arena) {
    dtb_node_state_t* state = (dtb_node_state_t*)dtb_arena_alloc(arena, sizeof(dtb_node_state_t));
    if (!state) return NULL;
    
    state->node_id = node_id;
    atomic_init(&state->hlc_physical, dtb_get_monotonic_time_ns() / 1000000ULL);
    atomic_init(&state->hlc_logical, 0);
    atomic_init(&state->is_partitioned, false);
    
    for (int i = 0; i < DTB_MAX_CLUSTER_NODES; i++) {
        atomic_init(&state->peer_weights[i], 100); /* Default balanced weight */
    }
    
    return state;
}

void dtb_hlc_tick(dtb_node_state_t* state, uint64_t physical_time_ms) {
    uint64_t phys = atomic_load_explicit(&state->hlc_physical, memory_order_relaxed);
    uint64_t log = atomic_load_explicit(&state->hlc_logical, memory_order_relaxed);
    
    while (1) {
        uint64_t new_phys = phys;
        uint64_t new_log = log;
        uint64_t current_physical_ms = physical_time_ms;
        
        if (current_physical_ms > new_phys) {
            new_phys = current_physical_ms;
            new_log = 0;
        } else {
            new_log++;
        }
        
        if (atomic_compare_exchange_weak_explicit(&state->hlc_physical, &phys, new_phys,
            memory_order_relaxed, memory_order_relaxed)) {
            atomic_store_explicit(&state->hlc_logical, new_log, memory_order_relaxed);
            break;
        }
        log = atomic_load_explicit(&state->hlc_logical, memory_order_relaxed);
    }
}

void dtb_hlc_update(dtb_node_state_t* state, uint64_t remote_physical_ms, uint64_t remote_logical) {
    uint64_t phys = atomic_load_explicit(&state->hlc_physical, memory_order_relaxed);
    uint64_t log = atomic_load_explicit(&state->hlc_logical, memory_order_relaxed);
    uint64_t local_physical_ms = dtb_get_monotonic_time_ns() / 1000000ULL;
    
    while (1) {
        uint64_t max_physical = phys;
        if (remote_physical_ms > max_physical) max_physical = remote_physical_ms;
        if (local_physical_ms > max_physical) max_physical = local_physical_ms;
        
        uint64_t new_phys = max_physical;
        uint64_t new_log = 0;
        
        if (new_phys == phys && new_phys == remote_physical_ms) {
            if (log > remote_logical) {
                new_log = log + 1;
            } else {
                new_log = remote_logical + 1;
            }
        } else if (new_phys == phys) {
            new_log = log + 1;
        } else if (new_phys == remote_physical_ms) {
            new_log = remote_logical + 1;
        } else {
            new_log = 0;
        }
        
        int64_t skew = (int64_t)remote_physical_ms - (int64_t)local_physical_ms;
        if (skew < 0) skew = -skew;
        
        if (skew > 5) { /* Epsilon > 5ms */
            atomic_store_explicit(&state->is_partitioned, true, memory_order_relaxed);
        }
        
        if (atomic_compare_exchange_weak_explicit(&state->hlc_physical, &phys, new_phys,
            memory_order_relaxed, memory_order_relaxed)) {
            atomic_store_explicit(&state->hlc_logical, new_log, memory_order_relaxed);
            break;
        }
        log = atomic_load_explicit(&state->hlc_logical, memory_order_relaxed);
    }
}

void dtb_hazard_init(dtb_hazard_t* hp) {
    for (int i = 0; i < DTB_MAX_HAZARD_POINTERS; i++) {
        atomic_init(&hp->pointers[i], NULL);
    }
}

void dtb_hazard_set(dtb_hazard_t* hp, int index, void* ptr) {
    if (index >= 0 && index < DTB_MAX_HAZARD_POINTERS) {
        atomic_store_explicit(&hp->pointers[index], ptr, memory_order_seq_cst);
    }
}

void dtb_hazard_clear(dtb_hazard_t* hp, int index) {
    if (index >= 0 && index < DTB_MAX_HAZARD_POINTERS) {
        atomic_store_explicit(&hp->pointers[index], NULL, memory_order_release