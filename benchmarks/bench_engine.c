#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <immintrin.h>

#define ITERATIONS 2000000
#define WARMUP_ITERATIONS 200000
#define BUCKET_CAPACITY 1000000.0
#define REFILL_RATE 100000.0

typedef struct {
    double capacity;
    double tokens;
    double refill_rate;
    uint64_t last_refill_timestamp;
    pthread_mutex_t lock;
} token_bucket_t;

static inline uint64_t get_nanos(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void bucket_init(token_bucket_t *b, double capacity, double rate) {
    b->capacity = capacity;
    b->tokens = capacity;
    b->refill_rate = rate;
    b->last_refill_timestamp = get_nanos();
    pthread_mutex_init(&b->lock, NULL);
}

static void bucket_destroy(token_bucket_t *b) {
    pthread_mutex_destroy(&b->lock);
}

static bool bucket_try_consume(token_bucket_t *b, double tokens_requested) {
    pthread_mutex_lock(&b->lock);
    
    uint64_t now = get_nanos();
    uint64_t elapsed_ns = now - b->last_refill_timestamp;
    
    if (elapsed_ns > 0) {
        double elapsed_sec = (double)elapsed_ns / 1e9;
        double generated = elapsed_sec * b->refill_rate;
        b->tokens = fmin(b->capacity, b->tokens + generated);
        b->last_refill_timestamp = now;
    }
    
    bool success = false;
    if (b->tokens >= tokens_requested) {
        b->tokens -= tokens_requested;
        success = true;
    }
    
    pthread_mutex_unlock(&b->lock);
    return success;
}

static int compare_uint64(const void *a, const void *b) {
    uint64_t fa = *(const uint64_t *)a;
    uint64_t fb = *(const uint64_t *)b;
    return (fa > fb) - (fa < fb);
}

int main(void) {
    token_bucket_t bucket;
    bucket_init(&bucket, BUCKET_CAPACITY, REFILL_RATE);

    uint64_t *latencies = malloc(ITERATIONS * sizeof(uint64_t));
    if (!latencies) {
        perror("malloc failed");
        return 1;
    }

    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        bucket_try_consume(&bucket, 1.0);
    }

    uint64_t start_time = get_nanos();

    for (int i = 0; i < ITERATIONS; i++) {
        uint64_t op_start = get_nanos();
        volatile bool res = bucket_try_consume(&bucket, 1.0);
        uint64_t op_end = get_nanos();
        (void)res;
        latencies[i] = op_end - op_start;
    }

    uint64_t end_time = get_nanos();
    uint64_t total_duration_ns = end_time - start_time;

    qsort(latencies, ITERATIONS, sizeof(uint64_t), compare_uint64);

    uint64_t p50 = latencies[(size_t)(ITERATIONS * 0.50)];
    uint64_t p90 = latencies[(size_t)(ITERATIONS * 0.90)];
    uint64_t p99 = latencies[(size_t)(ITERATIONS * 0.99)];
    uint64_t p999 = latencies[(size_t)(ITERATIONS * 0.999)];

    double duration_sec = (double)total_duration_ns / 1e9;
    double ops_sec = (double)ITERATIONS / duration_sec;

    printf("+--------------------------+----------------------+\n");
    printf("| Metric                   | Value                |\n");
    printf("+--------------------------+----------------------+\n");
    printf("| Total Iterations         | %-20d |\n", ITERATIONS);
    printf("| Duration (sec)           | %-20.6f |\n", duration_sec);
    printf("| Throughput (ops/sec)     | %-20.2f |\n", ops_sec);
    printf("| Latency P50 (ns)         | %-20llu |\n", (unsigned long long)p50);
    printf("| Latency P90 (ns)         | %-20llu |\n", (unsigned long long)p90);
    printf("| Latency P99 (ns)         | %-20llu |\n", (unsigned long long)p99);
    printf("| Latency P99.9 (ns)       | %-20llu |\n", (unsigned long long)p999);
    printf("+--------------------------+----------------------+\n");

    free(latencies);
    bucket_destroy(&bucket);

    return 0;
}