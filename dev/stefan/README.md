# Project Stefan

GPU acceleration for Trino query execution using NVIDIA RAPIDS cuDF.

Stefan is a GPU acceleration layer for Trino that leverages NVIDIA GPUs to accelerate
SQL query execution, providing order-of-magnitude performance improvements for
analytical workloads including filters, aggregations, joins, and sorts.

## Why "Stefan"?

Named after Stefan Banach (1892-1945), founder of modern functional analysis.

GPU acceleration works by transforming database operations into vector and matrix
computations - the mathematical domain Banach formalized. His work on operations
in high-dimensional vector spaces is exactly what GPU architectures optimize for.

## Status

🔬 **Research & Planning Phase**

The project is currently in the research and early planning stage. We are evaluating
GPU acceleration approaches, defining architecture, and preparing for prototyping.

## Goals

Target performance improvements for GPU-accelerated operations:
- Medium queries (seconds): 3-5x speedup
- Large analytical queries (minutes): 5-15x speedup
- Large joins/aggregations: 10-20x speedup

Actual performance will depend on query characteristics, data size, and GPU hardware.

## Non-Goals

These are some non-goals:

- accelerating all queries in all workloads
- pure GPU execution

CPU execution remains the primary path.

## Architecture

Stefan follows a **hybrid plugin architecture** similar to Apache Spark's RAPIDS plugin:

- **Optional GPU acceleration**: Queries can run on CPU or GPU based on cost estimates
- **Operator-level acceleration**: Individual operators (filter, join, aggregate) are GPU-accelerated
- **Automatic fallback**: Falls back to CPU execution on GPU errors or resource exhaustion
- **cuDF integration**: Uses NVIDIA RAPIDS cuDF library for GPU operations
- **Zero-copy transfers**: Optimizes data movement between CPU and GPU memory

Key components:
- GPU Resource Manager: Device management, memory allocation, multi-GPU support
- Data Conversion Layer: Bidirectional Page ↔ cuDF Table conversion
- GPU Operator Framework: GPU-aware operator interfaces and factories
- Cost Model: Intelligent GPU vs CPU selection based on data characteristics

## Technology Stack

- **NVIDIA RAPIDS cuDF**: Columnar GPU dataframe library
- **CUDA**: Version 11.0+ required
- **cuDF**: Java bindings for cuDF C++ library
- **Platform**: GPU support initially for Linux only

## Documentation

- [GPU Acceleration Research](gpu-acceleration-research.md) - Comprehensive research on GPU acceleration in data processing systems
- [Implementation Plan & JIRA Tickets](gpu-acceleration-jira-tickets.md) - Detailed implementation roadmap with 24 work items across 6 phases

## Getting Started

🚧 Coming soon - build instructions, development setup, and testing guidelines.
