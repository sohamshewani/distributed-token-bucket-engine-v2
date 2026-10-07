#define _GNU_SOURCE
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <immintrin.h>
#include <time.h>
#include <errno.h>

#define DTB_CACHE_LINE 64
#define DTB_ARENA_ALIGN 64
#define DTB_HAZARD_PTR_MAX 32
#define DTB_RING_BUFFER_ALIGN 64

typedef struct {
    _Atomic uint64_t tokens;
    _Atomic uint64_t last_refill_ns;
    _Atomic uint64_t sliding_windows[4];
    uint8_t padding[DTB_CACHE_LINE - (sizeof(_Atomic uint64_t) * 6)];
} dtb_bucket_t;

_Static_assert(sizeof(dtb_bucket_t) == DTB_CACHE_LINE, "dtb_bucket_t must be exactly 64 bytes");

typedef struct {
    uint32_t capacity;
    uint32_t mask;
    _Atomic uint64_t head;
    _Atomic uint64_t tail;
    void* buffer[];
} dtb_ring_buffer_t;

typedef struct {
    uint8_t* memory_block;
    size_t capacity;
    _Atomic size_t offset;
} dtb_arena_t;

typedef struct {
    _Atomic(void*) ptr;
} dtb_hazard_record_t;

typedef struct {
    dtb_hazard_record_t records[DTB_HAZARD_PTR_MAX];
} dtb_hazard_domain_t;

typedef struct {
    uint64_t physical_time_ns;
    uint64_t logical_counter;
    uint32_t node_id;
    _Atomic bool active;
    uint8_t padding[DTB_CACHE_LINE - (sizeof(uint64_t) * 2 + sizeof(uint32_t) + sizeof(_Atomic bool))];
} dtb_node_state_t;

static dtb_hazard_domain_t g_hazard_domain;
static _Thread_local dtb_hazard_record_t* tls_hazard_record = NULL;

static inline void cpu_relax(void) {
    __builtin_ia32_pause();
}

dtb_arena_t* dtb_arena_create(size_t capacity) {
    size_t aligned_capacity = (capacity + DTB_ARENA_ALIGN - 1) & ~(DTB_ARENA_ALIGN - 1);
    size_t total_size = sizeof(dtb_arena_t) + aligned_capacity;
    
    uint8_t* raw = NULL;
    if (posix_memalign((void**)&raw, DTB_ARENA_ALIGN, total_size) != 0) {
        return NULL;
    }
    
    dtb_arena_t* arena = (dtb_arena_t*)raw;
    arena->memory_block = raw + sizeof(dtb_arena_t);
    arena->capacity = aligned_capacity;
    atomic_init(&arena->offset, 0);
    
    return arena;
}

void dtb_arena_destroy(dtb_arena_t* arena) {
    if (arena) {
        free(arena);
    }
}

void* dtb_arena_alloc(dtb_arena_t* arena, size_t size, size_t alignment) {
    if (!arena || size == 0) {
        return NULL;
    }
    
    if (alignment < 8) {
        alignment = 8;
    }
    
    size_t current_offset, next_offset;
    void* ptr = NULL;
    
    while (true) {
        current_offset = atomic_load_explicit(&arena->offset, memory_order_relaxed);
        size_t padding = (alignment - (current_offset % alignment)) % alignment;
        size_t total_allocation = size + padding;
        
        if (current_offset + total_allocation > arena->capacity) {
            return NULL;
        }
        
        next_offset = current_offset + total_allocation;
        
        if (atomic_compare_exchange_weak_explicit(&arena->offset,
                                                  &current_offset,
                                                  next_offset,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            ptr = (void*)(arena->memory_block + current_offset + padding);
            break;
        }
    }
    
    memset(ptr, 0, size);
    return ptr;
}

void dtb_arena_reset(dtb_arena_t* arena) {
    if (arena) {
        atomic_store_explicit(&arena->offset, 0, memory_order_release);
    }
}

dtb_ring_buffer_t* dtb_ring_buffer_create(uint32_t capacity) {
    if ((capacity & (capacity - 1)) != 0 || capacity == 0) {
        return NULL;
    }
    
    size_t total_size = sizeof(dtb_ring_buffer_t) + (sizeof(void*) * capacity);
    void* raw = NULL;
    if (posix_memalign(&raw, DTB_RING_BUFFER_ALIGN, total_size) != 0) {
        return NULL;
    }
    
    dtb_ring_buffer_t* rb = (dtb_ring_buffer_t*)raw;
    rb->capacity = capacity;
    rb->mask = capacity - 1;
    atomic_init(&rb->head, 0);
    atomic_init(&rb->tail, 0);
    
    memset(rb->buffer, 0, sizeof(void*) * capacity);
    return rb;
}

void dtb_ring_buffer_destroy(dtb_ring_buffer_t* rb) {
    if (rb) {
        free(rb);
    }
}

bool dtb_ring_buffer_push(dtb_ring_buffer_t* rb, void* item) {
    if (!rb || !item) {
        return false;
    }
    
    uint64_t head = atomic_load_explicit(&rb->head, memory_order_relaxed);
    uint64_t tail;
    
    while (true) {
        tail = atomic_load_explicit(&rb->tail, memory_order_acquire);
        if ((head - tail) >= rb->capacity) {
            return false;
        }
        
        if (atomic_compare_exchange_weak_explicit(&rb->head,
                                                  &head,
                                                  head + 1,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            break;
        }
    }
    
    rb->buffer[head & rb->mask] = item;
    
    while (atomic_load_explicit(&rb->tail, memory_order_relaxed) != head) {
        cpu_relax();
    }
    atomic_store_explicit(&rb->tail, head + 1, memory_order_release);
    
    return true;
}

bool dtb_ring_buffer_pop(dtb_ring_buffer_t* rb, void** item) {
    if (!rb || !item) {
        return false;
    }
    
    uint64_t tail = atomic_load_explicit(&rb->tail, memory_order_relaxed);
    uint64_t head;
    
    while (true) {
        head = atomic_load_explicit(&rb->head, memory_order_acquire);
        if (tail == head) {
            return false;
        }
        
        if (atomic_compare_exchange_weak_explicit(&rb->tail,
                                                  &tail,
                                                  tail + 1,
                                                  memory_order_release,
                                                  memory_order_relaxed)) {
            break;
        }
    }
    
    *item = rb->buffer[tail & rb->mask];
    rb->buffer[tail & rb->mask] = NULL;
    return true;
}

void dtb_bucket_init(dtb_bucket_t* bucket, uint64_t initial_tokens, uint64_t current_time_ns) {
    if (!bucket) {
        return;
    }
    atomic_init(&bucket->tokens, initial_tokens);
    atomic_init(&bucket->last_refill_ns, current_time_ns);
    for (int i = 0; i < 4; i++) {
        atomic_init(&bucket->sliding_windows[i], 0);
    }
}

bool dtb_bucket_consume(dtb_bucket_t* bucket, uint64_t requested_tokens, uint64_t capacity, uint64_t refill_rate_per_ns, uint64_t current_time_ns) {
    if (!bucket || requested_tokens == 0) {
        return false;
    }
    
    uint64_t current_tokens = atomic_load_explicit(&bucket->tokens, memory_order_relaxed);
    uint64_t last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
    
    while (true) {
        uint64_t time_delta = (current_time_ns > last_refill) ? (current_time_ns - last_refill) : 0;
        uint64_t generated_tokens = time_delta * refill_rate_per_ns;
        
        uint64_t potential_tokens = current_tokens + generated_tokens;
        if (potential_tokens > capacity) {
            potential_tokens = capacity;
        }
        
        if (potential_tokens < requested_tokens) {
            return false;
        }
        
        uint64_t remaining_tokens = potential_tokens - requested_tokens;
        
        if (atomic_compare_exchange_weak_explicit(&bucket->tokens,
                                                  &current_tokens,
                                                  remaining_tokens,
                                                  memory_order_acq_rel,
                                                  memory_order_relaxed)) {
            
            uint64_t expected_refill = last_refill;
            atomic_compare_exchange_strong_explicit(&bucket->last_refill_ns,
                                                    &expected_refill,
                                                    current_time_ns,
                                                    memory_order_release,
                                                    memory_order_relaxed);
            return true;
        }
        
        cpu_relax();
        last_refill = atomic_load_explicit(&bucket->last_refill_ns, memory_order_relaxed);
    }
}

void dtb_hazard_init(void) {
    for (int i = 0; i < DTB_HAZARD_PTR_MAX; i++) {
        atomic_init(&g_hazard_domain.records[i].ptr, NULL);
    }
}

dtb_hazard_record_t* dtb_hazard_acquire_record(void) {
    if (tls_hazard_record != NULL) {
        return tls_hazard_record;
    }
    
    for (int i = 0; i < DTB_HAZARD_PTR_MAX; i++) {
        void* expected = NULL;
        if (atomic_compare_exchange_strong_explicit(&g_hazard_domain.records[i].ptr,
                                                    &expected,
                                                    (void*)0x1,
                                                    memory_order_acquire,
                                                    memory_order_relaxed)) {
            tls_hazard_record = &g_hazard_domain.records[i];
            atomic_store_explicit(&tls_hazard_record->ptr, NULL, memory_order_release);
            return tls_hazard_record;
        }
    }
    return NULL;
}

void dtb_hazard_release_record(dtb_hazard_record_t* record) {
    if (!record) {
        return;
    }
    atomic_store_explicit(&record->ptr, NULL, memory_order_release);
    if (tls_hazard_record == record) {
        tls_hazard_record = NULL;
    }
}

void dtb_hazard_set(dtb_hazard_record_t* record, void* ptr) {
    if (record) {
        atomic_store_explicit(&record->ptr, ptr, memory_order_release);
    }
}

bool dtb_node_state_reclaim_check(void* node_ptr) {
    for (int i = 0; i < DTB_HAZARD_PTR_MAX; i++) {
        void* protected_ptr = atomic_load_explicit(&g_hazard_domain.records[i].ptr, memory_order_acquire);
        if (protected_ptr == node_ptr) {
            return false;
        }
    }
    return true;
}

void dtb_node_state_init(dtb_node_state_t* node, uint32_t node_id, uint64_t initial_time_ns) {
    if (!node) {
        return;
    }
    node->node_id = node_id;
    node->physical_time_ns = initial_time_ns;
    node->logical_counter = 0;
    atomic_init(&node->active, true);
    memset(node->padding, 0, sizeof(node->padding));
}

bool dtb_hlc_advance(dtb_node_state_t* node, uint64_t physical_now_ns, uint64_t remote_physical_ns, uint64_t remote_logical_counter, uint64_t epsilon_ns, double* correction_factor) {
    if (!node) {
        return false;
    }
    
    uint64_t local_physical = node->physical_time_ns;
    uint64_t max_physical = local_physical;
    
    if (physical_now_ns > max_physical) {
        max_physical = physical_now_ns;
    }
    if (remote_physical_ns > max_physical) {
        max_physical = remote_physical_ns;
    }
    
    if (max_physical == local_physical && max_physical == remote_physical_ns) {
        if (remote_logical_counter > node->logical_counter) {
            node->logical_counter = remote_logical_counter + 1;
        } else {
            node->logical_counter++;
        }
    } else if (max_physical == remote_physical_ns) {
        if (remote_physical_ns > local_physical) {
            node->physical_time_ns = remote_physical_ns;
            node->logical_counter = remote_logical_counter + 1;
        } else {
            node->logical_counter++;
        }
    } else {
        if (max_physical > local_physical) {
            node->physical_time_ns = max_physical;
            node->logical_counter = 0;
        } else {
            node->logical_counter++;
        }
    }
    
    int64_t skew = (int64_t)physical_now_ns - (int64_t)remote_physical_ns;
    if (skew < 0) {
        skew = -skew;
    }
    
    if ((uint64_t)skew > epsilon_ns) {
        if (correction_factor) {
            *correction_factor = 0.5;
        }
        return true;
    } else {
        if (correction_factor) {
            *correction_factor = 1.0;
        }
        return false;
    }
}