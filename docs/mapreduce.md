# MapReduce Implementation

This document describes the design and execution flow of the MapReduce implementation used in this project.

## Overview

MapReduce is a programming model for processing datasets by dividing computation into two primary stages:

1. **Map**
2. **Reduce**

The implementation separates computation from coordination.

Workers perform application-level computation, while the coordinator manages the execution of tasks.

---

## Map Phase

The Map phase reads input data and transforms it into intermediate key-value pairs.

For example:

```text
hello world hello
```

A mapper could produce:

```text
(hello, 1)
(world, 1)
(hello, 1)
```

These intermediate values must then be routed to reducers.

## Intermediate Data Partitioning

Intermediate key-value pairs are partitioned using:

```text
hash(key) % number_of_reducers
```

The resulting partition determines which reducer will process the key.

This ensures that every intermediate value associated with the same key is sent to the same reducer.

For example:

```text
hash("hello") % NReduce -> reducer 1
hash("world") % NReduce -> reducer 2
```

All occurrences of `"hello"` will therefore be handled by reducer 1.

---

## Reduce Phase

The Reduce phase reads the intermediate data assigned to its partition.

Values are grouped by key before the user-defined Reduce function is executed.

For example:

```text
hello -> [1, 1]
world -> [1]
```

A Word Count reducer could produce:

```text
hello 2
world 1
```

The resulting values are written to the final MapReduce output files.

---

# Architecture

The implementation contains three main components:

## Coordinator

The coordinator manages the execution of the MapReduce job.

Its responsibilities include:

* Creating Map tasks
* Assigning tasks to workers
* Tracking task state
* Detecting completed tasks
* Managing the transition between phases
* Creating Reduce tasks
* Monitoring worker progress

The coordinator does **not** perform the application-level Map or Reduce computation.

Its responsibility is coordination.

---

## Worker

Workers perform the actual computation.

A worker repeatedly communicates with the coordinator to obtain work.

Its responsibilities include:

* Requesting tasks
* Executing Map functions
* Executing Reduce functions
* Creating intermediate files
* Reading intermediate files
* Producing final output
* Reporting task completion

Multiple workers can execute tasks concurrently.

---

## Mapper

When a worker receives a Map task, it:

1. Reads the assigned input file.
2. Applies the application's Map function.
3. Receives a collection of intermediate key-value pairs.
4. Determines the reducer partition for each key.
5. Writes the intermediate values to the appropriate intermediate files.
6. Reports completion to the coordinator.

Partition selection is performed using:

```text
hash(key) % number_of_reducers
```

---

## Reducer

When a worker receives a Reduce task, it:

1. Identifies the intermediate data associated with its reducer partition.
2. Reads the intermediate key-value pairs.
3. Merges the input.
4. Sorts the data by key.
5. Groups values belonging to the same key.
6. Applies the application's Reduce function.
7. Writes the final output.

---

# Execution Flow

A MapReduce job follows approximately this sequence:

```text
Input Files
     |
     v
+-------------+
| Coordinator |
+-------------+
     |
     | Assign Map Tasks
     v
+-------------+
|   Workers   |
+-------------+
     |
     | Map()
     v
Intermediate Key-Value Pairs
     |
     | Partition by hash(key)
     v
Intermediate Files
     |
     | Map phase completes
     v
+-------------+
| Coordinator |
+-------------+
     |
     | Assign Reduce Tasks
     v
+-------------+
|   Workers   |
+-------------+
     |
     | Reduce()
     v
Final Output
```

The coordinator controls the phase transition.

Reduce tasks should begin only after the required Map tasks have completed and their intermediate output is available.

---

# Communication

Workers and the coordinator communicate using **RPC**.

RPC allows workers to:

* Register with the system
* Request work
* Receive task metadata
* Notify the coordinator when work has completed

The coordinator maintains shared task state and uses synchronization primitives such as mutex locks to safely handle requests from concurrent workers.

---

# Task State Management

Tasks move through execution states as workers process them.

Conceptually, a task may be:

```text
Pending -> Running -> Completed
```

The coordinator uses these states to determine:

* Which tasks can be assigned
* Which tasks are currently executing
* Whether the Map phase has completed
* Whether the Reduce phase has completed
* Whether the entire MapReduce job is finished

---

# MapReduce Applications

The MapReduce engine is separated from application-specific logic.

Applications provide their own Map and Reduce functions.

For example, a Word Count application might define:

```go
func Map(filename string, contents string) []KeyValue
```

and:

```go
func Reduce(key string, values []string) string
```

Applications inside `mrapps` are compiled as Go plugins.

Example:

```bash
go build -buildmode=plugin ../mrapps/wc.go
```

The resulting plugin can then be loaded by the worker.

This allows the MapReduce infrastructure to remain generic while different applications provide different computation logic.

---

# Design Principle

One of the most important lessons from the implementation is:

> **The coordinator should coordinate, not compute.**

Workers own the data-processing responsibilities.

The coordinator manages:

* Scheduling
* State
* Coordination
* Phase transitions

Keeping these responsibilities separate makes the architecture easier to reason about and provides a stronger foundation for adding fault tolerance and more advanced distributed-system behavior.

---

# Future Improvements

The next major improvements are focused on reliability and observability.

## Worker Failure Recovery

Detect workers that stop responding while executing a task.

## Task Timeouts

Mark long-running tasks as available again after a timeout.

## Task Reassignment

Allow another worker to execute a task when the original worker fails.

## Fault Tolerance

Improve the system's ability to recover from partial failures without restarting the entire job.

## Testing

Add more comprehensive tests covering:

* Concurrent workers
* Phase transitions
* Partition correctness
* Worker failures
* Duplicate task execution
* Intermediate file handling

## Performance Benchmarking

Measure:

* Map execution time
* Reduce execution time
* Worker utilization
* Scheduling overhead
* Performance as the number of workers increases
