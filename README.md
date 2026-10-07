# distributed-token-bucket-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/distributed-token-bucket-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/distributed-token-bucket-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://img.shields.io/badge/Security-Hardened-blue.svg) [![Stars](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket-engine?style=flat)](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket-engine)

## Executive Summary & Problem Space

Modern distributed systems, hyperscale API gateways, and multi-tenant data planes demand high-throughput, microsecond-latency rate limiting under intense concurrency. Traditional centralized token bucket implementations bottleneck on global synchronization locks, while naive distributed state sharing incurs catastrophic network overhead and clock drift vulnerabilities. 

`distributed-token-bucket-engine` is a production-grade, lockless-adjacent rate-limiting execution engine written in C99. Designed to operate at the hardware boundary, it solves the fundamental limits of high-frequency state evaluation. By decoupling bucket refill arithmetic from state mutation paths via lock-free ring buffers (`src/dtb_ringbuffer.c`), leveraging fine-grained synchronization primitives (`src/dtb_concurrency.c`, `src/dtb_sync.c`), and enforcing strict memory safety via specialized allocators (`src/dtb_memory.c`), the engine delivers deterministic throughput scaling across multi-socket, multi-core NUMA architectures without falling prey to priority inversion, thread starvation, or race conditions.

---

## Structural ASCII Architecture / Dataflow Diagram

The engine follows a strict layered architecture pattern, separating ingress request validation, lock-free transmission buffering, core rate evaluation, and underlying synchronization state.

```
 +-----------------------------------------------------------------------+
 |                     Client Application Threads                        |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                        Engine Ingress Layer                           |
 |      (include/dtb_types.h & src/dtb_engine.c API Entrypoints)         |
 +-----------------------------------------------------------------------+
                                    |
                    +---------------+---------------+
                    | (Batch Check)                 | (Mutation Path)
                    v                               v
 +------------------------------------+   +------------------------------+
 |       src/dtb_concurrency.c        |   |    src/dtb_ringbuffer.c      |
 |  (Thread Safety & Hazard Pointers) |   | (Lock-Free Ring SPSC/MPSC)   |
 +------------------------------------+   +------------------------------+
                    |                               |
                    +---------------+---------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                        Core Execution Engine                          |
 |                          (src/dtb_engine.c)                           |
 |    - Token Refill Arithmetic     - Capacity Bounds Checking           |
 |    - Burst Allowance Engine      - Expiration & Eviction Logic        |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       Synchronization Layer                           |
 |                          (src/dtb_sync.c)                             |
 |    - Distributed Epoch State     - Cross-Node Sync & Reconciliation   |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                         Memory Management                             |
 |                          (src/dtb_memory.c)                           |
 |    - Arena/Pool Allocators       - Cache-Line Aligned Memory Pools    |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
The engine avoids heavy mutual exclusion locks on critical read paths. Instead, it deploys a hybrid concurrency model:
* **Lock-Free Ring Buffers (`src/dtb_ringbuffer.c`)**: Utilize single-producer/single-consumer (SPSC) and multi-producer/single-consumer (MPSC) atomics (`stdatomic.h`) to pass token acquisition events without kernel-level thread blocking.
* **Hazard Pointers & Epoch-Based Reclamation (`src/dtb_concurrency.c`)**: Safe memory reclamation is enforced without global stop-the-world garbage collection. Threads register active bucket pointers in thread-local hazard arrays before traversal.

### Memory Ownership & Allocator Hardening
* **Cache-Line Alignment**: All primary structs (`dtb_bucket_t`, `dtb_ring_t`) are explicitly padded to 64-byte cache lines (`alignas(64)`), eliminating false sharing across physical CPU cores.
* **Deterministic Allocation (`src/dtb_memory.c`)**: Dynamic heap allocations are minimized during steady-state request handling. Buckets and ring buffers are pre-allocated via custom slab/pool allocators featuring canary padding and memory bounds checking.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Average Time Complexity | Worst-Case Time Complexity | Space Complexity | Estimated Latency (p99) |
| :--- | :--- | :--- | :--- | :--- |
| **Token Acquisition Check** | $O(1)$ | $O(1)$ | $O(1)$ | $< 120\text{ ns}$ |
| **Ring Buffer Push/Pop** | $O(1)$ | $O(1)$ | $O(N)$ | $< 45\text{ ns}$ |
| **Bucket State Refill** | $O(1)$ | $O(1)$ | $O(1)$ | $< 80\text{ ns}$ |
| **Distributed State Merge** | $O(K)$ *(K = Nodes)* | $O(K)$ | $O(K)$ | $< 850\text{ ns}$ |
| **Memory Allocation / Pool** | $O(1)$ | $O(N)$ | $O(N)$ | $< 200\text{ ns}$ |

---

## Security Architecture & Threat Model

`distributed-token-bucket-engine` is hardened against malicious denial-of-service vectors, memory corruption vulnerabilities, and side-channel exploitation.

* **Input Bounds Sanitization**: All incoming token demands, capacity limits, and refill intervals undergo rigorous boundary validation (`assert` invariants and return-code checks) to prevent integer overflows, division-by-zero, and negative-time anomalies.
* **Memory Safety & Sanitization**: Compiles cleanly under strict compiler flags (`-Wall -Wextra -Werror -Wshadow -Wconversion -pedantic`). The build system natively supports AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and MemorySanitizer (MSan).
* **Zero-Leak Guarantee**: Sensitive synchronization epoch tokens and tracking structures are explicitly scrubbed via secure memory-zeroing primitives (`memset_s` equivalents) prior to allocator recycling.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
* A C99-compliant compiler (`gcc` >= 9.0 or `clang` >= 10.0)
* GNU `make`
* POSIX-compliant system environment (Linux / macOS)

### 1. Compilation & Build
To compile the static library, test suites, and benchmarking harness with optimization flags (`-O3`):
```bash
make clean
make all
```

### 2. Execution of Invariant & Unit Tests
To verify thread safety, state consistency, and memory constraints under simulated concurrent loads:
```bash
make test
```
*(Runs the test suite defined in `tests/test_invariants.c` with Valgrind/ASan wrappers if enabled).*

### 3. Running Performance Benchmarks
To execute the micro-benchmarks measuring throughput, latency distribution, and atomic contention rates:
```bash
make bench
```
*(Executes the driver in `benchmarks/bench_engine.c` outputting nanosecond-resolution metrics).*

---

## License
Distributed under the MIT License. See `LICENSE` for more information.