# MapReduce Implementation in Go

A custom implementation of the **MapReduce programming model** built in Go as part of my journey through **MIT 6.5840 Distributed Systems**.

The goal of this project is to understand the core ideas behind distributed computation: task scheduling, worker coordination, data partitioning, and parallel processing.

---

## Overview

MapReduce is a distributed programming model designed to process large datasets by dividing computation into two main phases:

## Map Phase

The Map phase reads input data and transforms it into intermediate key-value pairs.

Example:

```
Input:
hello world hello
```

Mapper output:

```
(hello, 1)
(world, 1)
(hello, 1)
```

The intermediate data is partitioned using:

```
hash(key) % number_of_reducers
```

This ensures that all values belonging to the same key are routed to the same reducer.

---

## Reduce Phase

The Reduce phase collects intermediate values belonging to the same key, groups them, and produces the final output.

Example:

```
Input:

hello -> [1, 1]
world -> [1]
```

Reducer output:

```
hello 2
world 1
```

---

# Architecture

The implementation consists of three main components:

## Coordinator

The coordinator is responsible for:

- Managing map and reduce tasks
- Assigning tasks to workers
- Tracking task states
- Handling phase transitions
- Monitoring worker progress

The coordinator does not process application data.

Its responsibility is to coordinate the execution.

---

## Worker

Workers are responsible for:

- Requesting tasks from the coordinator
- Executing map and reduce functions
- Generating intermediate files
- Reporting task completion

Workers perform the actual computation.

---

## Mapper

The mapper:

1. Reads input files
2. Applies the user-defined map function
3. Generates intermediate key-value pairs
4. Partitions data using:

```
hash(key) % number_of_reducers
```

5. Writes intermediate files for reducers

---

## Reducer

The reducer:

1. Discovers intermediate files assigned to its partition
2. Reads and merges key-value pairs
3. Sorts data by key
4. Applies the reduce function
5. Produces final output

---

# Data Flow

```
                 +-------------+
                 | Coordinator |
                 +-------------+
                       |
          Assign Map / Reduce Tasks
                       |
        +--------------+--------------+
        |                             |
        v                             v
   +---------+                  +---------+
   | Worker  |                  | Worker  |
   | Mapper  |                  | Reducer |
   +---------+                  +---------+
        |                             |
        | Intermediate Files          |
        +------------+----------------+
                     |
                     v
              Final Output
```

---

# Building MapReduce Plugins

MapReduce applications are implemented as Go plugins inside the mrapps directory.

For example, the Word Count application can be compiled as a plugin using:

``` 
go build -buildmode=plugin ../mrapps/wc.go
```

This generates the plugin:
```
wc.so
```
inside the main directory. Multiple MapReduce applications can be built in the same way by replacing wc.go with the corresponding application file.

# Key Concepts Implemented

- Distributed task scheduling
- RPC-based communication
- Concurrent worker execution
- Map and Reduce phases
- Intermediate data partitioning
- Worker monitoring
- Task state management
- Synchronization using mutex locks

---

# Technologies

- Language: Go
- Communication: RPC
- Course: MIT 6.5840 Distributed Systems

---

# Current Progress

✅ Coordinator implementation  
✅ Worker registration  
✅ Map task scheduling  
✅ Intermediate key-value generation  
✅ Data partitioning  
✅ Reduce task scheduling  
✅ Reduce execution flow

---

# Lessons Learned

One of the biggest design lessons from this project:

> The coordinator should coordinate, not compute.

Workers execute tasks and handle data processing.

The coordinator only manages workflow, assigns tasks, and keeps the system moving.

This separation of responsibilities is one of the foundations of scalable distributed systems.

---

# Future Improvements

- Worker failure recovery
- Task timeout and reassignment
- More extensive testing
- Performance benchmarking
- Improved fault tolerance

---

# Acknowledgment

Built while studying **MIT 6.5840 Distributed Systems** and exploring the foundations of distributed computing.