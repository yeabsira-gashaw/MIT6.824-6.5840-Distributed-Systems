# MapReduce in Go

A custom implementation of the **MapReduce programming model** in Go, built while studying **MIT 6.5840 Distributed Systems**.

The project is focused on understanding the foundations of distributed computation, including task scheduling, worker coordination, RPC communication, data partitioning, synchronization, and parallel processing.

## Features

* Coordinator-based task scheduling
* RPC communication between workers and coordinator
* Concurrent worker execution
* Map and Reduce phases
* Intermediate key-value generation
* Hash-based data partitioning
* Reduce task scheduling
* Task state management
* Mutex-based synchronization

## Project Structure

The system consists primarily of:

* **Coordinator** — assigns tasks and manages execution state
* **Workers** — execute Map and Reduce tasks
* **MapReduce applications** — user-defined Map and Reduce functions compiled as Go plugins

For a detailed explanation of the architecture and execution flow, see:

[`docs/mapreduce.md`](docs/mapreduce.md)

## Building MapReduce Applications

MapReduce applications are located inside the `mrapps` directory.

For example, to build the Word Count application:

```bash
go build -buildmode=plugin ../mrapps/wc.go
```

This generates:

```text
wc.so
```

Other applications can be compiled in the same way by replacing `wc.go` with the desired application file.

## Technologies

* **Language:** Go
* **Communication:** RPC
* **Concurrency:** Goroutines and mutex synchronization
* **Course:** MIT 6.5840 Distributed Systems

## Current Progress

* ✅ Coordinator implementation
* ✅ Worker registration
* ✅ Map task scheduling
* ✅ Intermediate key-value generation
* ✅ Data partitioning
* ✅ Reduce task scheduling
* ✅ Reduce execution flow

## Future Improvements

* Worker failure recovery
* Task timeout and reassignment
* Improved fault tolerance
* More extensive testing
* Performance benchmarking

## Acknowledgment

Built while studying **MIT 6.5840 Distributed Systems** and exploring the foundations of distributed systems and parallel computation.
