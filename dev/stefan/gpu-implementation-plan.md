# Trino GPU Acceleration - Implementation Plan

**Date:** 2026-03-27
**Status:** 🚧 Phase 1 - In Progress
**Last Updated:** 2026-04-03

---

## Implementation Status

### ✅ Completed (Phase 1 - Foundation)

- **Core Data Structures**:
  - `GpuPage` - Data model with `positionCount()`, `columnCount()`, `column(index)`, `columns()` methods
  - `Column` - Sealed interface: `Blocks` (CPU), `DeviceMemory` (GPU) - two-level hierarchy
  - `Mask` - Sealed interface (exists but NOT integrated into GpuPage)
  - `RuntimeCloseable` - Helper interface for AutoCloseable without checked exceptions

- **Operation Framework**:
  - `GpuOperation` - Pull-based interface with nested `Result` sealed interface
  - `GpuOperation.Factory` - Factory pattern for creating operations
  - `GpuOperator` - Full implementation of Trino `Operator` interface with Factory
  - `GpuSourceOperation` - Interface for source operations with `needsInput()`, `addInput()`, `noMoreInput()`

- **Ownership Management**:
  - `@Borrow` - Annotation for borrowed references (no ownership transfer)
  - `@Move` - Annotation for ownership transfer
  - `@Own` - Annotation for owned local variables/fields
  - Required on ALL methods passing ColumnVector or GpuPage
  - Widely used throughout codebase

- **Planning Integration**:
  - `GpuScore` enum - POTENTIAL vs PREFERRED scoring
  - `GpuExpressionCompiler` integrated into `LocalExecutionPlanner.visitScanFilterAndProject()`
  - **Direct GPU operator creation** (no parallel pipeline tracking)
  - Simple pattern: if GPU filter compiles → create GpuOperator, else CPU FilterAndProjectOperator

- **Expression Compilation Layer**:
  - `GpuExpressionCompiler` class - Compiles RowExpressions to GPU operations (cuDF-based)
  - `GpuExpression` interface - Evaluates expressions on GPU
  - `CompiledExpression` record - Bundles GpuExpression + input channels + GpuScore
  - `InputChannels` class - Tracks required input channels
  - Scoring heuristics: LIKE/regex → PREFERRED
  - **Working implementations**: `InputReferenceExpression`, `LIKE` function
  - `GpuLike` - Full LIKE pattern matching with cuDF

- **GPU Operations**:
  - `GpuFilter` - Filters rows using boolean mask and cuDF Table.filter()
  - `GpuProject` - Projects columns with PassThrough and Gpu evaluation modes
  - `BufferPages` - Batches input Pages (100K row threshold) into GpuPage with CPU Blocks
  - `CopyToDevice` - Selective CPU→GPU transfer (only specified columns converted to DeviceMemory)
  - `CopyToBlocks` - GPU→CPU conversion with batched block building (16-row batches)
  - All operations use ownership annotations correctly

- **Type Support**:
  - ✅ **VARCHAR** - Full support in CopyToDevice and CopyToBlocks
  - ⏸️ Other primitive types (BIGINT, INTEGER, DOUBLE, etc.) - stubs present, not yet implemented

- **Testing**:
  - ✅ `TestDistributedGpuEngineOnlyQueries` - Test infrastructure working
  - ✅ **End-to-end LIKE test passing** - VARCHAR filter with LIKE pattern on GPU
  - Property: `gpu-acceleration.enabled=true`

### 🔄 Architectural Differences from Original Plan

**Implemented Pattern:**
```
BufferPages (batching)
  → CopyToDevice (selective CPU→GPU)
  → GpuFilter/GpuProject (operations)
  → CopyToBlocks (GPU→CPU)
```

**Key Differences:**
1. ❌ **No `PhysicalOperation` extensions** - no parallel CPU/GPU pipeline tracking
2. ❌ **No incremental weaving** - direct GpuOperator creation in LocalExecutionPlanner
3. ✅ **Explicit conversion stages** - clearer boundaries than planned implicit conversions
4. ✅ **Simpler factory pattern** - direct `GpuOperator.Factory` instead of accumulation
5. ❌ **No Mask in GpuPage** - Mask interface exists but separate, not integrated
6. ❌ **No HostMemory layer** - direct Blocks→DeviceMemory (2-level instead of 3-level)

### 📝 In Progress

- Expression compilation for more operators (arithmetic, comparisons, string functions)
- Support for additional primitive types (BIGINT, INTEGER, DOUBLE, DATE, etc.)
- Configuration framework (currently inline property check)

### 🔜 TODO (Phase 1)

- GPU memory management infrastructure (currently basic incRefCount/close)
- More expression types: SpecialForm (AND/OR/CASE), Constants, arithmetic operators
- Projection operation integration into LocalExecutionPlanner
- More comprehensive test coverage
- Performance benchmarking framework
- Configuration class (GpuConfig)
- Monitoring and metrics

---

## Overview

This document describes the implementation approach for GPU acceleration in Trino using NVIDIA RAPIDS cuDF library. The
design follows Spark RAPIDS patterns while adapting to Trino's architecture.

### Core Approach

- **Single uber GPU operator** injected by LocalExecutionPlanner
- **Batch accumulation** before GPU invocation (memory/row-based threshold)
- **GPU Pages** - hybrid data structure with CPU and GPU memory blocks
- **Fail-fast** - no CPU fallback for GPU operations
- **Gradual operation coverage** - add GPU operations incrementally

---

## 1. GPU Page Abstraction

### 1.1 Design

`GpuPage` extends the concept of Trino's `Page` to support blocks in both CPU and GPU memory.

```java
// GpuPage - data spanning CPU and GPU memory
public final class GpuPage implements RuntimeCloseable {
    private final int positionCount;
    private final Column[] columns;

    public int positionCount()                       // Modern API (no "get" prefix)
    public int columnCount()                         // Modern API (no "get" prefix)
    public List<@Borrow Column> columns()            // NEW: Access all columns
    public @Borrow Column column(int index)          // Modern API (no "get" prefix)

    public GpuPage(int positionCount, @Borrow Column[] columns)  // Takes ownership via incRefCount
}

// Column - sealed interface for CPU/GPU memory
public sealed interface Column extends RuntimeCloseable {
    int positionCount();

    // Regular CPU memory (Trino blocks)
    final class Blocks implements Column {
        public Blocks(List<Block> blocks)
        public List<Block> blocks()
        @Override void close()  // No-op (blocks are GC'd)
    }

    // GPU device memory (reference-counted)
    final class DeviceMemory implements Column {
        private final @Own ColumnVector columnVector;

        public DeviceMemory(ColumnVector columnVector)
        public @Borrow ColumnVector columnVector()
        @Override synchronized void close()  // Decrements refcount, frees if last
    }
}
```

**Implementation Status:** ✅ Fully Implemented (Updated 2026-04-03)

**Key Design Decisions:**

- **Two-level memory hierarchy**: `Blocks` (CPU) → `DeviceMemory` (GPU)
  - **Note**: Original plan had 3 levels (Blocks → HostMemory → DeviceMemory), simplified to 2
- **Modern API naming**: No "get" prefix on property-like methods (`positionCount()` vs `getPositionCount()`)
- **Bulk column access**: New `columns()` method returns unmodifiable list view
- **Ownership transfer in constructor**: Constructor takes `@Borrow Column[]` but creates owned copies
  - `Blocks` just stored as-is (no ownership, GC'd)
  - `DeviceMemory` calls `incRefCount()` to create independent reference
- **RuntimeCloseable interface**: All resources implement this (AutoCloseable without checked exceptions)
- **Reference counting**: DeviceMemory uses cuDF's internal reference counting
- **Direct transfer**: CPU blocks → GPU device memory (no intermediate pinned host memory)


### 1.2 Block Lifecycle Management

**Ownership Model:**

- `GpuPage` owns all channels (CPU and GPU) and therefore owns GPU data
- Reference counting for GPU memory blocks (similar to cuDF ColumnVector)
- CPU Blocks can be shared across GpuPages (they are garbage collected)

**Memory Management:**

- Each `GpuChannel` wraps a cuDF `ColumnVector`
- Reference count incremented on share, decremented on release
- GPU memory freed when refcount reaches zero
- CPU-side metadata retained for query execution tracking

### 1.3 Ownership Annotations

**Implementation Status:** ✅ Implemented

To prevent memory leaks and use-after-free bugs with GPU resources, all methods passing `ColumnVector` or `GpuPage` **must** use ownership annotations:

**Annotations:**

```java
@Retention(SOURCE)
@Target({PARAMETER, METHOD})
public @interface Borrow {}  // Ownership NOT transferred

@Retention(SOURCE)
@Target({PARAMETER, METHOD})
public @interface Move {}    // Ownership IS transferred
```

**Rules:**

1. **Every parameter and return value** passing GPU resources must be annotated with `@Borrow` or `@Move`
2. **@Borrow** = Caller retains ownership, callee must NOT close the resource
3. **@Move** = Ownership transferred, receiver must close the resource
4. **Default is forbidden** - unannotated GPU resource passing is a bug

**Examples:**

```java
// Filter borrows input, returns new owned GpuPage
@Move GpuPage filter(@Borrow GpuPage input, Expression predicate) {
    // input is borrowed - DO NOT close it
    // Create new GpuPage with filter applied
    GpuPage result = applyFilter(input, predicate);
    return result;  // Caller owns result, must close it
}

// Expression evaluation borrows inputs, returns owned result
@Move ColumnVector evaluate(@Borrow List<ColumnVector> inputs, @Borrow Mask mask) {
    // inputs and mask are borrowed - DO NOT close them
    ColumnVector result = inputs.get(0).add(inputs.get(1));
    return result;  // Caller must close result
}

// Getter returns borrowed reference
@Borrow ColumnVector getColumn(int index) {
    return columns.get(index);  // Caller must NOT close this
}

// Incorrect - missing annotation (compile error desired)
ColumnVector process(ColumnVector input) {  // ❌ ERROR: Missing @Borrow or @Move
    // ...
}
```

**Ownership Patterns:**

1. **Pipeline pattern**: Operations borrow input, return owned output
   ```java
   @Move GpuPage op(@Borrow GpuPage input)
   ```

2. **Consumer pattern**: Operation takes ownership, returns nothing
   ```java
   void consume(@Move GpuPage input) {
       try (input) { /* use */ }
   }
   ```

3. **Builder pattern**: Operation creates and returns owned resource
   ```java
   @Move GpuPage build()
   ```

4. **Accessor pattern**: Returns borrowed reference to internal resource
   ```java
   @Borrow ColumnVector get(int index)
   ```

**Implementation Enforcement:**

- Annotations are source-only (`@Retention(SOURCE)`)
- No runtime overhead
- Annotations are `@Documented` for javadoc generation
- Also applicable to `TYPE_USE` for generic type parameters (e.g., `List<@Borrow ColumnVector>`)
- Future: Add error-prone or IntelliJ inspections to enforce annotations
- Code review must check all GPU resource passing is annotated

**Design Decision: `@Borrow` for Operation Inputs**

Filter and projection operations use `@Borrow` for input parameters:
```java
@Move GpuPage evaluate(@Borrow GpuPage input);
```

**Rationale:**
- **Robustness**: Caller manages lifecycle with try-with-resources, ensuring cleanup on failures
- **Reusability**: Input remains valid after operation, can be reused or debugged
- **Reference counting**: Operations call `incRefCount()` on columns they keep, explicit ownership transfer
- **Pipeline safety**: Each stage can fail independently without corrupting caller's resources

**Implementation requirement**: All operations must call `incRefCount()` on input columns that are retained in the output.

---

## 2. GPU Operator Design

**Key Architectural Patterns:**

1. **Pull Model**: Operations form a chain where each pulls from its predecessor, similar to Trino's `Operator` interface. Enables natural backpressure and lazy evaluation.

2. **Result State Machine**: Operations return `GpuOperation.Result` sealed interface with states: `Data(GpuPage)`, `Blocked(future)`, `Yielded()`, `Finished()`. State propagates naturally through the chain.

3. **Batching at Source**: `GpuSourceOperation` accumulates input Pages until thresholds met, then creates batches. Rest of pipeline operates on batched data.

4. **Lazy GPU Transfer**: Columns start as `Blocks` (CPU memory), transferred to `DeviceMemory` (GPU) only when operations require them. Minimizes transfer overhead.

### 2.1 GpuOperator class

**Implementation Status:** ✅ Implemented (basic structure)

**Structure:**

```java
public class GpuOperator implements Operator {
    private final OperatorContext operatorContext;
    private final GpuOperation topOperation;
    private final GpuSourceOperation sourceOperation;
    private boolean finished;
    
    @Override
    public void addInput(Page page) {
        checkState(!finished, "operator is finished");
        checkState(needsInput(), "operator does not need input");
        sourceOperation.addInput(page);
    }
    
    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }
        
        GpuOperation.Result result = topOperation.execute();
        return switch (result) {
            case GpuOperation.Data(GpuPage gpuPage) -> {
                // TODO: Convert GpuPage back to CPU Page
                yield null;
            }
            case GpuOperation.Blocked ignored -> null;
            case GpuOperation.Yielded ignored -> null;
            case GpuOperation.Finished ignored -> {
                finished = true;
                yield null;
            }
        };
    }
    
    @Override
    public ListenableFuture<Void> isBlocked() {
        GpuOperation.Result result = topOperation.execute();
        return switch (result) {
            case GpuOperation.Blocked(ListenableFuture<Void> future) -> future;
            case GpuOperation.Data ignored,
                 GpuOperation.Yielded ignored,
                 GpuOperation.Finished ignored -> NOT_BLOCKED;
        };
    }
    
    @Override
    public boolean needsInput() {
        return !finished && sourceOperation.needsInput();
    }
    
    @Override
    public void finish() {
        sourceOperation.noMoreInput();
    }
    
    @Override
    public boolean isFinished() {
        return finished || topOperation.isFinished();
    }
    
    // GpuSourceOperation marker interface
    public interface GpuSourceOperation extends GpuOperation {
        void addInput(Page page);
        void noMoreInput();
        boolean needsInput();
    }
}
```

**Key Points:**

- Implements standard Trino `Operator` interface
- Pulls from `topOperation.execute()` in both `getOutput()` and `isBlocked()`
- Converts between `GpuOperation.Result` states and `Operator` protocol
- `GpuSourceOperation` interface defines batching contract
- TODO: GpuPage → Page conversion not yet implemented


### 2.2 Batch Accumulation Strategy

**Initial Implementation:**

- Accumulate incoming Pages until threshold reached:
    - **Row threshold**: 1M rows (configurable)
    - **Memory threshold**: 100MB uncompressed (configurable)
    - **Timeout**: 100ms max wait (prevents starvation)
- Whichever threshold hit first triggers GPU execution

**Accumulation Logic:**

```
addInput(Page page):
    buffer.add(page)
    if (buffer.rowCount >= ROW_THRESHOLD ||
        buffer.memorySize >= MEMORY_THRESHOLD ||
        timeSinceFirstPage > TIMEOUT) {
        executeOnGpu()
    }

needsInput():
    return !isFinished() && buffer.size < THRESHOLD
```

**Partial Batching:**

- A batch can be 1 Page or N Pages - conceptually identical
- Driver calls getOutput() when needs data, we flush current batch
- No special handling needed - standard Page-based flow

### Batching and Dictionary or RLE blocks

Initial implementation of `GpuOperator` is not able to carry `DictionaryBlock` and `RunLengthEncodedBlock` instances
but will not operate on them.
`GpuOperator` maintains set of indices of input data it will eventually transfer to GPU.
If incoming data is dictionary- or RLE-encoded, it's processed by falling back to CPU execution pipeline.
For that purpose, `GpuOperator` maintains a pure-CPU based fallback.


### 2.3 GPU Execution Flow - Pull Model

**Operator-Level Flow (Trino Driver → GpuOperator):**

```
1. Driver calls addInput(Page) repeatedly
   → GpuSourceOperation accumulates Pages in buffer
   
2. Driver calls getOutput()
   → GpuOperator calls topOperation.execute(context)
   
3. Pull propagates down operation chain:
   topOperation.execute()
     → intermediateOp.execute()
       → sourceOperation.execute()
       
4. GpuSourceOperation checks if batch ready:
   - Row threshold reached (e.g., 1M rows)
   - Memory threshold reached (e.g., 100MB)
   - Timeout reached (e.g., 100ms since first Page)
   - Input finished (no more Pages coming)
   
   If ready: Convert buffered Pages → GpuPage, return Data(GpuPage)
   If not ready: Return Blocked(future) or Yielded()
   
5. Data flows up operation chain:
   sourceOp returns Data(GpuPage) 
     → filter applies mask or materializes
     → project computes new columns (GPU)
     → aggregate groups and aggregates (GPU)
     → topOp returns Data(GpuPage)
     
6. GpuOperator converts GpuPage → CPU Page
   - Materialize GPU blocks back to CPU
   - Apply any remaining masks
   - Return Page to Driver
   
7. Report GPU memory usage to MemoryPool
```

**State Propagation (`GpuOperation.Result` states):**

- `Blocked(future)`: Operation waiting (e.g., insufficient batch, GPU busy)
- `Yielded()`: Operation yielding CPU to avoid starvation
- `Finished()`: No more data will be produced
- `Data(GpuPage)`: Data available for processing

**Lazy Evaluation Benefits:**

- Masks avoid materializing filtered data until necessary
- Columns transferred to GPU only when actually accessed
- Operations can be fused (e.g., filter + project combined)
- Memory pressure handled naturally via pull backpressure

---

## 3. `LocalExecutionPlanner` Integration

### 3.1 PhysicalOperation Extensions

**Implementation Status:** ✅ Completed

`PhysicalOperation` now maintains parallel CPU and GPU pipelines:

```java
private static class PhysicalOperation {
    private final List<OperatorFactory> pipelineTail;              // CPU pipeline (always present)
    private final Optional<List<OperatorFactory>> gpuPipelineTail; // GPU alternative pipeline
    private final Optional<GpuScore> gpuPipelineScore;             // GPU quality score
    private final Map<TableHandle, List<OperatorFactory>> pipelineHeadAlternatives;
    private final Optional<PlanNodeId> chooseAlternativePlanNodeId;
    private final Map<Symbol, Integer> layout;
    private final List<Type> types;
}

enum GpuScore {
    POTENTIAL,   // Can run on GPU, but not necessarily beneficial
    PREFERRED    // Expected to benefit from GPU (e.g., LIKE, regex operations)
}
```

**Key Design:**
- Both pipelines maintained in parallel during planning
- `gpuPipelineTail` and `gpuPipelineScore` are both present or both absent (validated in constructor)
- Final decision (CPU vs GPU) made at DriverFactory creation time

### 3.2 Incremental GPU Pipeline Building

**Pattern:** Each `visitXXX()` method builds GPU pipeline incrementally by:

1. **Always create CPU operator** (required for correctness)
2. **Try GPU compilation** of expressions using `GpuExpressionCompiler`
3. **Weave into existing GPU pipeline** if compilation succeeds:
   - If last operator in `gpuPipelineTail` is `GpuOperatorFactory`: add operation to it (fusion)
   - Otherwise: create new `GpuOperatorFactory` and append to pipeline
4. **Propagate or upgrade score:**
   - If new operation is PREFERRED, entire pipeline becomes PREFERRED
   - If pipeline was already PREFERRED, stays PREFERRED
5. **Return PhysicalOperation** with both CPU and GPU pipelines

**Weaving Example:**
```
Initial state: source has GPU pipeline with GpuOperatorFactory
visitFilter: compiles filter, adds to existing GpuOperatorFactory
visitProject: compiles project, adds to same GpuOperatorFactory
Result: Single GpuOperator containing [source → filter → project] chain
```

## 2. Operation Pipeline Architecture

### 2.1 Actual Implementation (Explicit Conversion Stages)

**Pipeline Structure:**
```
Input: Trino Page (CPU)
  ↓
BufferPages (batches to ~100K rows)
  ↓ produces GpuPage with Column.Blocks
CopyToDevice (selective CPU→GPU transfer)
  ↓ converts specified columns to Column.DeviceMemory
GpuFilter / GpuProject (GPU operations)
  ↓ operates on DeviceMemory columns
CopyToBlocks (GPU→CPU conversion)
  ↓ converts DeviceMemory back to Blocks
Output: GpuPage with Column.Blocks → converted to Trino Page
```

**Key Components:**

1. **BufferPages** (`implements GpuSourceOperation`)
   - Accumulates input Pages until threshold: 100,000 rows (configurable)
   - Combines into single GpuPage with all columns as `Column.Blocks`
   - Returns `Yielded` when waiting, `Data(GpuPage)` when ready, `Finished` when done

2. **CopyToDevice** (`implements GpuOperation`)
   - Takes GpuPage with `Blocks` columns
   - Selectively transfers specified columns to GPU (Column.DeviceMemory)
   - Leaves unneeded columns as Blocks (zero-copy passthrough)
   - **Currently implemented**: VARCHAR type only
   - Uses HostColumnVector.builder() → buildAndPutOnDevice()

3. **GpuFilter / GpuProject** (core GPU operations)
   - Operate on DeviceMemory columns
   - Use cuDF operations (Table.filter(), ColumnVector operations)
   - Return new GpuPage with DeviceMemory columns

4. **CopyToBlocks** (`implements GpuOperation`)
   - Converts DeviceMemory columns back to Blocks
   - Batched copying (16 rows at a time) for efficiency
   - Uses ColumnVector.copyToHost() → HostColumnVector
   - Builds Trino Blocks via BlockBuilder
   - **Currently implemented**: VARCHAR type only

**Benefits of Explicit Conversion:**
- Clear boundaries between CPU and GPU memory
- Easy to debug (can inspect at each stage)
- Selective transfer (only needed columns go to GPU)
- Simpler than implicit lazy conversion

### 2.2 Original Plan (Not Implemented)

**Original Design:** Lazy transfer with three-level memory hierarchy and masks
- Planned: Blocks → HostMemory (pinned) → DeviceMemory (GPU)
- Planned: Mask integration into GpuPage for lazy filtering
- Planned: PhysicalOperation with parallel CPU/GPU pipelines
- Planned: Incremental weaving in LocalExecutionPlanner

**Why Changed:** Explicit conversion stages proved simpler and easier to reason about

---

## 3. LocalExecutionPlanner Integration

### 3.1 Actual Implementation (Direct Creation)

**Pattern in `visitScanFilterAndProject`:**

```java
public PhysicalOperation visitScanFilterAndProject(
        LocalExecutionPlanContext context,
        PlanNodeId planNodeId,
        PlanNode sourceNode,
        Optional<Expression> filterExpression,
        Assignments assignments,
        List<Symbol> outputSymbols)
{
    // Get source operation (recursive)
    PhysicalOperation source = sourceNode.accept(this, context);
    Map<Symbol, Integer> sourceLayout = source.getLayout();
    
    // Translate to RowExpressions (Trino IR)
    Optional<RowExpression> translatedFilter = 
        filterExpression.map(filter -> toRowExpression(filter, sourceLayout));
    List<RowExpression> translatedProjections = 
        projections.stream()
            .map(expr -> toRowExpression(expr, sourceLayout))
            .collect(toImmutableList());
    
    // === CPU PATH (always) ===
    Function<InternalDynamicFilter, PageProcessor> pageProcessor = 
        expressionCompiler.compilePageProcessor(...);
    
    OperatorFactory cpuOperator = FilterAndProjectOperator.createOperatorFactory(
        context.getNextOperatorId(),
        planNodeId,
        () -> pageProcessor.apply(dynamicFilter),
        getTypes(projections),
        ...);
    
    // === GPU PATH (optional) ===
    Optional<List<OperatorFactory>> gpuPipeline = source.getGpuPipelineTail();
    Optional<GpuScore> gpuScore = source.getGpuPipelineScore();
    
    if (isGpuEnabled(session) && source != null) {
        // Compile filter expression
        Optional<CompiledExpression> compiledFilter = translatedFilter.isPresent()
            ? gpuExpressionCompiler.compileExpression(translatedFilter.get())
            : Optional.empty();
        
        // Compile each projection expression
        List<CompiledExpression> compiledProjections = new ArrayList<>();
        for (RowExpression projection : translatedProjections) {
            Optional<CompiledExpression> compiled = gpuExpressionCompiler.compileExpression(projection);
            if (compiled.isEmpty()) {
                // Cannot compile all projections, skip GPU for this node
                compiledProjections.clear();
                break;
            }
            compiledProjections.add(compiled.get());
        }
        
        // If either compiled successfully, weave into GPU pipeline
        if (compiledFilter.isPresent() || !compiledProjections.isEmpty()) {
            GpuOperation tail = source.getGpuPipelineTail();
            GpuScore maxScore = source.getGpuPipelineScore().orElse(POTENTIAL);
            
            if (compiledFilter.isPresent()) {
                tail = new GpuFilterOperation(tail, compiledFilter.get());
                maxScore = max(maxScore, compiledFilter.get().score());
            }
            if (!compiledProjections.isEmpty()) {
                List<GpuProjectOperation.Projection> projections = buildProjections(compiledProjections);
                tail = new GpuProjectOperation(tail, projections);
                maxScore = compiledProjections.stream()
                        .map(CompiledExpression::score)
                        .reduce(maxScore, Ordering.natural()::max);
            }
            
            // Weave: check if last operator is GpuOperatorFactory
            if (gpuPipeline.isPresent() && !gpuPipeline.get().isEmpty()) {
                OperatorFactory lastOp = Iterables.getLast(gpuPipeline.get());
                if (lastOp instanceof GpuOperatorFactory gpuOpFactory) {
                    // Add operations to existing GpuOperator (fusion)
                    newOps.forEach(gpuOpFactory::addOperation);
                    // Pipeline unchanged, score upgraded
                    gpuScore = Optional.of(maxScore);
                }
                else {
                    // Start new GpuOperator
                    GpuOperatorFactory newGpuOpFactory = new GpuOperatorFactory(...);
                    newOps.forEach(newGpuOpFactory::addOperation);
                    gpuPipeline = Optional.of(ImmutableList.<OperatorFactory>builder()
                        .addAll(gpuPipeline.get())
                        .add(newGpuOpFactory)
                        .build());
                    gpuScore = Optional.of(maxScore);
                }
            }
            else {
                // No existing GPU pipeline, start new one
                GpuOperatorFactory gpuOpFactory = new GpuOperatorFactory(...);
                newOps.forEach(gpuOpFactory::addOperation);
                gpuPipeline = Optional.of(ImmutableList.of(gpuOpFactory));
                gpuScore = Optional.of(maxScore);
            }
        }
    }
    
    // Build PhysicalOperation with both pipelines
    return new PhysicalOperation(
        ImmutableList.<OperatorFactory>builder()
            .addAll(source.getPipelineTail())
            .add(cpuOperator)
            .build(),
        gpuPipeline,
        gpuScore,
        ImmutableMap.of(),
        Optional.empty(),
        outputMappings);
}
```

### 3.4 Expression Compilation Layer

**Implementation Status:** ✅ Interfaces defined, stub implementation complete

**Purpose:** Translates Trino RowExpressions (IR) into GPU-executable evaluators.

**Architecture:**
```
RowExpression (Trino IR)
    ↓
GpuExpressionCompiler.compileExpression()
    ↓
Optional<CompiledExpression>
    ↓ (used by)
GpuFilterOperation / GpuProjectOperation
    ↓ (added to)
GpuOperatorFactory
    ↓ (creates)
GpuOperator (chains operations at runtime)
```

**Compiler Class:**

```java
// GPU expression compiler (concrete class, cuDF-based)
public class GpuExpressionCompiler {
    public Optional<CompiledExpression> compileExpression(RowExpression expression);
    
    public record CompiledExpression(
            GpuExpression expression,
            InputChannels inputChannels,
            GpuScore score) {}
}

// Expression evaluator interface (execute on GPU at runtime)
interface GpuExpression {
    @Move ColumnVector evaluate(List<@Borrow ColumnVector> inputColumns);
}
```

**Scoring Heuristics (Phase 1):**

- **PREFERRED**: Expression contains LIKE, `regexp_like`, `regexp_extract`, `regexp_replace`, or other regex operations
  - Rationale: String pattern matching is highly compute-intensive, benefits significantly from GPU parallelism
- **POTENTIAL**: All other expressions (comparisons, arithmetic, simple functions)
  - Rationale: May benefit from GPU but overhead might dominate for small datasets

**Implementation (GpuExpressionCompiler class):**

```java
public class GpuExpressionCompiler {
    public Optional<CompiledExpression> compileExpression(RowExpression expression) {
        return expression.accept(new CompilationVisitor(), null);
    }
    
    public record CompiledExpression(
            GpuExpression expression,
            InputChannels inputChannels,
            GpuScore score) {}
    
    private static class CompilationVisitor 
            implements RowExpressionVisitor<Optional<CompiledExpression>, Void> {
        
        @Override
        public Optional<CompiledExpression> visitInputReference(InputReferenceExpression reference, Void context) {
            // ✅ IMPLEMENTED - Pass through input column
            int channel = reference.field();
            return Optional.of(new CompiledExpression(
                    inputColumns -> getOnlyElement(inputColumns).incRefCount(),
                    InputChannels.of(channel),
                    POTENTIAL));
        }
        
        @Override
        public Optional<CompiledExpression> visitCall(CallExpression call, Void context) {
            CatalogSchemaFunctionName functionName = call.resolvedFunction().signature().getName();
            
            GpuScore score = POTENTIAL;
            if (functionName.equals(builtinFunctionName(LIKE_FUNCTION_NAME))) {
                score = PREFERRED;  // LIKE is compute-intensive, benefits from GPU
            }
            
            // TODO: Implement cuDF code generation for function calls
            return Optional.empty();
        }
        
        // TODO: visitSpecialForm, visitConstant, visitLambda, visitVariableReference
    }
}
```

**Phase 1 Status:**
- ✅ GpuExpression interface defined
- ✅ CompiledExpression record bundling expression + input channels + score
- ✅ Compiler framework with scoring heuristics
- ✅ InputReference compilation implemented
- ⏳ Actual cuDF code generation for functions/operators (next step)

**Return Value:**
- `Optional<CompiledExpression>` containing expression + metadata if compilation succeeds
- `Optional.empty()` if expression not GPU-compatible or not yet implemented

---

## 3.1 GPU Operations

GPU operations implement the `GpuOperation` interface and execute filter/project logic on the GPU.

### GpuFilterOperation

Applies a boolean mask to filter rows using cuDF's `Table.filter()` method.

```java
public class GpuFilterOperation implements GpuOperation {
    private final GpuOperation source;
    private final CompiledExpression filter;
    
    public GpuFilterOperation(GpuOperation source, CompiledExpression filter) {
        this.source = source;
        this.filter = filter;
    }
    
    @Override
    public Result execute() {
        return switch (source.execute()) {
            case Data(GpuPage page) -> {
                try (page) {
                    yield processPage(page)
                            .<Result>map(Data::new)
                            .orElseGet(Yielded::new);  // Empty result after filtering
                }
            }
            // Pass through Blocked, Finished, Yielded
        };
    }
    
    private Optional<@Move GpuPage> processPage(@Borrow GpuPage input) {
        // 1. Evaluate filter expression to get boolean mask
        // 2. Count rows passing filter (sum of mask)
        // 3. If zero, return Optional.empty()
        // 4. Build Table from DeviceMemory columns
        // 5. Apply mask using table.filter(mask)
        // 6. Return new GpuPage with filtered columns
    }
}
```

**Key Features:**
- Returns `Optional.empty()` when filter eliminates all rows (avoids creating empty pages)
- Uses cuDF `Table.filter()` for efficient batch filtering
- Properly handles resource cleanup with try-with-resources
- Throws UnsupportedOperationException for Column.Blocks (CPU columns not yet supported)

### GpuProjectOperation

Projects columns using either pass-through or GPU expression evaluation.

```java
public class GpuProjectOperation implements GpuOperation {
    private final GpuOperation source;
    private final List<Projection> projections;
    
    public sealed interface Projection {
        record PassThrough(int sourceChannel) implements Projection {}
        record Gpu(CompiledExpression expression) implements Projection {}
    }
    
    @Override
    public Result execute() {
        return switch (source.execute()) {
            case Data(GpuPage page) -> {
                try (page) {
                    yield new Data(processPage(page));
                }
            }
            // Pass through Blocked, Finished, Yielded
        };
    }
    
    private @Move GpuPage processPage(@Borrow GpuPage input) {
        Column[] newColumns = new Column[projections.size()];
        try {
            for (int i = 0; i < projections.size(); i++) {
                newColumns[i] = switch (projections.get(i)) {
                    case PassThrough(int sourceChannel) -> {
                        // Pass through: incRefCount for DeviceMemory, return Blocks as-is
                        yield switch (input.getColumn(sourceChannel)) {
                            case Column.Blocks blocks -> blocks;
                            case Column.DeviceMemory(ColumnVector cv) -> 
                                new Column.DeviceMemory(cv.incRefCount());
                        };
                    }
                    case Gpu(CompiledExpression expr) -> {
                        // Evaluate GPU expression on required input columns
                        List<ColumnVector> inputs = expr.inputChannels().getInputChannels()
                                .stream()
                                .map(input::getColumn)
                                .map(Column.DeviceMemory.class::cast)
                                .map(Column.DeviceMemory::columnVector)
                                .collect(toImmutableList());
                        yield new Column.DeviceMemory(expr.expression().evaluate(inputs));
                    }
                };
            }
            return new GpuPage(input.getPositionCount(), newColumns);
        }
        finally {
            // Clean up on failure
            for (Column column : newColumns) {
                if (column != null) column.close();
            }
        }
    }
}
```

**Key Features:**
- Sealed `Projection` interface: `PassThrough` for identity, `Gpu` for computation
- Pass-through handles both CPU (Blocks) and GPU (DeviceMemory) columns
- GPU evaluation extracts required input columns and invokes compiled expression
- Proper ownership: borrows input, moves output
- Resource cleanup in finally block

**Implementation Status:** ✅ Both operations implemented with basic functionality

---

## 4. Page ↔ cuDF Conversion

### 4.1 Page → cuDF Table

Direct conversion without Arrow intermediate:

```java
class PageToCudfConverter {
    Table convert(Page page) {
        int columnCount = page.getChannelCount();
        ColumnVector[] columns = new ColumnVector[columnCount];

        for (int i = 0; i < columnCount; i++) {
            Block block = page.getBlock(i);
            columns[i] = convertBlock(block, page.getPositionCount());
        }

        return new Table(columns);
    }

    ColumnVector convertBlock(Block block, int positionCount) {
        Type type = block.getType();

        // Handle different block types
        if (block instanceof IntArrayBlock) {
            return convertIntBlock((IntArrayBlock) block, positionCount);
        } else if (block instanceof LongArrayBlock) {
            return convertLongBlock((LongArrayBlock) block, positionCount);
        } else if (block instanceof VariableWidthBlock) {
            return convertVariableWidthBlock((VariableWidthBlock) block, positionCount);
        }
        // ... more types
    }
}
```

### 4.2 cuDF Table → Page

```java
class CudfToPageConverter {
    Page convert(Table table) {
        int columnCount = table.getNumberOfColumns();
        Block[] blocks = new Block[columnCount];

        for (int i = 0; i < columnCount; i++) {
            ColumnVector column = table.getColumn(i);
            blocks[i] = convertColumn(column);
        }

        return new Page(table.getRowCount(), blocks);
    }
}
```

### 4.3 Type Mapping

| Trino Type | cuDF DType    | Notes                    |
|------------|---------------|--------------------------|
| BOOLEAN    | BOOL8         | Direct mapping           |
| TINYINT    | INT8          | Direct mapping           |
| SMALLINT   | INT16         | Direct mapping           |
| INTEGER    | INT32         | Direct mapping           |
| BIGINT     | INT64         | Direct mapping           |
| REAL       | FLOAT32       | Direct mapping           |
| DOUBLE     | FLOAT64       | Direct mapping           |
| VARCHAR    | STRING        | UTF-8 in cuDF            |
| VARBINARY  | LIST<UINT8>   | Byte array               |
| DATE       | INT32 (days)  | Days since epoch         |
| TIMESTAMP  | INT64 (μs)    | Microseconds since epoch |
| DECIMAL    | DECIMAL64/128 | Precision-dependent      |
| ARRAY      | LIST          | Nested type              |
| MAP        | STRUCT<LIST>  | Complex mapping          |
| ROW        | STRUCT        | Nested type              |

---

## 5. GPU Operations

### 5.1 Operation Interface - Pull Model

**Implementation Status:** ✅ Implemented

**GpuOperation Interface:**

```java
package io.trino.operator.gpu;

public interface GpuOperation {
    Result execute();
    List<Integer> getRequiredChannels();
    long estimateGpuMemory(long inputSizeInBytes);
    boolean isFinished();
    
    sealed interface Result {}
    
    record Blocked(ListenableFuture<Void> future) implements Result {
        public Blocked {
            requireNonNull(future, "future is null");
        }
    }
    
    record Finished() implements Result {}
    
    record Yielded() implements Result {}
    
    record Data(GpuPage page) implements Result {
        public Data {
            requireNonNull(page, "page is null");
        }
    }
}
```

**Pull Model Characteristics:**

- Each `GpuOperation` holds reference to preceding `GpuOperation` (or source)
- Calls `preceding.execute()` to pull data when needed
- Returns `GpuOperation.Result` to indicate state: data available, blocked, yielded, or finished
- Enables lazy evaluation and backpressure propagation
- Similar to Trino's `Operator` interface pattern

### 5.2 Example: GPU Filter with Pull Model

```java
class GpuFilterOperation implements GpuOperation {
    private final Expression predicate;
    private final GpuOperation source;  // Preceding operation
    private boolean finished = false;

    public GpuFilterOperation(Expression predicate, GpuOperation source) {
        this.predicate = predicate;
        this.source = source;
    }

    @Override
    public Result execute(CudaContext context) {
        if (finished) {
            return new Finished();
        }

        // Pull from preceding operation
        Result sourceResult = source.execute(context);

        return switch (sourceResult) {
            case Blocked blocked -> blocked;  // Propagate blocked state
            case Finished() -> {
                finished = true;
                yield new Finished();
            }
            case Yielded() -> new Yielded();  // Propagate yield
            case Data(GpuPage input) -> {
                // Execute filter operation - evaluate and materialize immediately
                ColumnVector filterMask = evaluatePredicate(predicate, input);
                Table filtered = input.asCudfTable().filter(filterMask);
                filterMask.close();
                yield new Data(GpuPage.fromTable(filtered));
            }
        };
    }

    @Override
    public List<Integer> getRequiredChannels() {
        // Extract column references from predicate
        return extractColumnReferences(predicate);
    }
    
    @Override
    public boolean isFinished() {
        return finished;
    }
}
```

### 5.3 GpuSourceOperation - Input Batching

The source operation sits at the bottom of the operation chain and handles batching of input Pages:

```java
class GpuSourceOperation implements GpuOperation {
    private final Queue<Page> inputBuffer = new ArrayDeque<>();
    private final long rowThreshold;
    private final long memoryThreshold;
    private final Duration timeout;
    
    private long bufferedRows = 0;
    private long bufferedBytes = 0;
    private Instant firstPageTime = null;
    private boolean noMoreInput = false;
    private boolean finished = false;
    
    @Override
    public Result execute(CudaContext context) {
        if (finished) {
            return new Finished();
        }
        
        // Check if batch is ready
        if (isBatchReady()) {
            GpuPage batch = createBatch();
            return new Data(batch);
        }
        
        // If no more input coming and buffer not empty, flush it
        if (noMoreInput && !inputBuffer.isEmpty()) {
            GpuPage batch = createBatch();
            if (inputBuffer.isEmpty()) {
                finished = true;
            }
            return new Data(batch);
        }
        
        // If no more input and buffer empty, finished
        if (noMoreInput) {
            finished = true;
            return new Finished();
        }
        
        // Not ready yet, return blocked or yielded
        return createBlockedFuture();
    }
    
    public void addInput(Page page) {
        inputBuffer.add(page);
        bufferedRows += page.getPositionCount();
        bufferedBytes += page.getRetainedSizeInBytes();
        
        if (firstPageTime == null) {
            firstPageTime = Instant.now();
        }
    }
    
    public void noMoreInput() {
        noMoreInput = true;
    }
    
    public boolean needsInput() {
        return !noMoreInput && !isBatchReady();
    }
    
    private boolean isBatchReady() {
        if (inputBuffer.isEmpty()) {
            return false;
        }
        
        // Check thresholds
        return bufferedRows >= rowThreshold
            || bufferedBytes >= memoryThreshold
            || (firstPageTime != null && 
                Duration.between(firstPageTime, Instant.now()).compareTo(timeout) > 0);
    }
    
    private GpuPage createBatch() {
        // Combine buffered Pages into single GpuPage
        List<Page> pages = new ArrayList<>();
        while (!inputBuffer.isEmpty() && shouldIncludeInBatch()) {
            pages.add(inputBuffer.poll());
        }
        
        GpuPage result = combinePages(pages);
        
        // Reset batch state
        bufferedRows = calculateBufferedRows();
        bufferedBytes = calculateBufferedBytes();
        if (inputBuffer.isEmpty()) {
            firstPageTime = null;
        } else {
            firstPageTime = Instant.now();
        }
        
        return result;
    }
    
    private GpuPage combinePages(List<Page> pages) {
        // Combine multiple Pages into single GpuPage
        // All data remains in CPU memory initially (CpuChannels)
        // Will be lazily transferred to GPU when accessed
        // ...
    }
    
    @Override
    public boolean isFinished() {
        return finished;
    }
    
    @Override
    public List<Integer> getRequiredChannels() {
        return List.of(); // Source doesn't require channels
    }
}
```

**Key Responsibilities:**

- Accept input Pages via `addInput()`
- Buffer Pages until batch thresholds met
- Combine buffered Pages into single GpuPage
- Return `Data(GpuPage)` when batch ready
- Return `Blocked` or `Yielded` when waiting for more input
- Track batch size, memory, and timeout

**Batching Strategy:**

- **Row threshold**: Batch when ≥ N rows (e.g., 1M)
- **Memory threshold**: Batch when ≥ M bytes (e.g., 100MB)
- **Timeout**: Batch after T milliseconds since first Page (e.g., 100ms)
- **Finish**: Batch remaining data when `noMoreInput()` called

### 5.4 Operation Pipeline - Pull Model

Operations form a chain where each pulls from its predecessor:

**Pipeline Structure:**
```
GpuSourceOperation (batches input Pages)
    ↑ pulls from
GpuFilterOperation (applies filter as mask or materializes)
    ↑ pulls from
GpuProjectOperation (computes new columns)
    ↑ pulls from
GpuAggregationOperation (groups and aggregates)
    ↑ pulls from
GpuOperator (top-level, called by Trino driver)
```

**Execution Flow (Pull Model):**

1. Trino Driver calls `gpuOperator.getOutput()`
2. GpuOperator calls `topOperation.execute(context)`
3. Top operation pulls from its source: `source.execute(context)`
4. Pull propagates down the chain until reaching GpuSourceOperation
5. GpuSourceOperation batches input Pages → returns `Data(GpuPage)`
6. Each operation processes data and returns result upstream
7. Data flows up the chain, operations can:
   - Apply lazy transformations (masks, column references)
   - Materialize GPU operations (transfers to GPU, executes cuDF)
   - Propagate `Blocked`, `Yielded`, or `Finished` states

**Advantages of Pull Model:**

- Natural backpressure: operations only pull when ready
- Lazy evaluation: can defer expensive GPU transfers
- State propagation: `Blocked`/`Yielded`/`Finished` flow naturally
- Memory control: limits in-flight data
- Matches Trino's `Operator` pattern

**Example Execution Sequence:**

```
Call: gpuOperator.getOutput()
  → aggregate.execute(ctx)
    → project.execute(ctx)  
      → filter.execute(ctx)
        → source.execute(ctx)
          → [batches Pages] → Data(GpuPage with CPU blocks)
        ← [applies mask] ← Data(GpuPage with mask)
      ← [computes columns] ← Data(GpuPage with GPU blocks)
    ← [aggregates] ← Data(GpuPage with GPU blocks)
  ← [converts to Page] ← returns CPU Page
```

---

## 6. Memory Management

### 6.1 GPU Memory Pool

Separate from Trino's CPU MemoryManager:

```java
class GpuMemoryManager {
    private final long maxGpuMemory;
    private final AtomicLong usedMemory;

    DeviceMemoryBuffer allocate(long bytes) {
        // Try allocation with cuDF RMM (RAPIDS Memory Manager)
        // RMM handles:
        // - Memory pooling
        // - Unified Virtual Memory (UVM) for spilling
        // - Fragmentation management
    }

    void reportToTrino() {
        // Report GPU memory usage to Trino's MemoryPool
        // for query admission control and stats
        memoryContext.setBytes(usedMemory.get());
    }
}
```

### 6.2 Integration with Trino Memory Management

```java
class GpuOperatorContext {
    private final DriverContext driverContext;
    private final LocalMemoryContext memoryContext;
    private final GpuMemoryManager gpuMemory;

    void updateMemoryUsage() {
        // Report both CPU and GPU memory
        long cpuBytes = calculateCpuMemory();
        long gpuBytes = gpuMemory.getUsedMemory();

        memoryContext.setBytes(cpuBytes + gpuBytes);
    }
}
```

### 6.3 Memory Pressure Handling

**Backpressure via Trino Memory System:**

- GPU operator reports retained memory (CPU buffers + GPU memory)
- If memory pool exhausted, Trino blocks operator.addInput()
- No special GPU-specific backpressure needed

**GPU Memory Exhaustion:**

- Rely on cuDF RMM with UVM (Unified Virtual Memory)
- RMM automatically spills to CPU memory when GPU full
- No explicit spilling logic in Trino code initially

**Future Optimization:**

- Explicit spilling of GpuPages to CPU
- Per-query GPU memory limits
- GPU memory reservation for large operations

---

## 7. Configuration

### 7.1 System Configuration (config.properties)

```properties
# Enable/disable GPU acceleration
gpu.enabled=false
# GPU device selection (comma-separated IDs)
gpu.device-ids=0,1,2,3
# Batch accumulation thresholds
gpu.batch-rows-threshold=1000000
gpu.batch-memory-threshold=100MB
gpu.batch-timeout=100ms
# GPU memory management
gpu.memory-pool-size=16GB
```

### 7.2 Session Properties

```sql
-- Enable GPU for session
SET SESSION enable_gpu_acceleration = true;
```

### 7.3 Configuration Classes

```java

@Config
public class GpuConfig {
    private boolean enabled = false;
    private List<Integer> deviceIds = ImmutableList.of(0);
    private long batchRowsThreshold = 1_000_000;
    private DataSize batchMemoryThreshold = DataSize.of(100, MEGABYTE);
    private Duration batchTimeout = Duration.ofMillis(100);
    private DataSize memoryPoolSize = DataSize.of(16, GIGABYTE);

    // Getters and setters with @Config annotations
}
```

---

## 8. Error Handling and Failure Modes

### 8.1 Fail-Fast Strategy

**No CPU fallback** - if GPU operation fails, fail the query:

```java
try{
result =

executeOnGpu(input);
}catch(
CudaException e){
        throw new

TrinoException(GPU_EXECUTION_ERROR,
        "GPU operation failed: "+e.getMessage(),e);
        }
```

**Rationale:**

- Simpler implementation (no dual code paths)
- Clear failure semantics
- Easier debugging
- User explicitly enables GPU, expects it to work

### 8.2 GPU Unavailable

If GPU not available at startup or GPU drivers missing:

- Set `gpu.enabled = false` automatically
- Log warning message
- All queries run on CPU

### 8.3 Error Categories

| Error Type            | Handling            | User Action                      |
|-----------------------|---------------------|----------------------------------|
| CUDA out of memory    | Fail query          | Reduce batch size or disable GPU |
| CUDA driver error     | Fail query          | Check GPU drivers                |
| GPU not found         | Disable GPU feature | Install GPU or disable config    |
| Unsupported operation | Fail query          | Disable GPU for this query       |
| cuDF exception        | Fail query          | Report bug with repro            |

---

## 9. Observability and Debugging

### 9.1 Metrics

Expose GPU-specific metrics:

```java
class GpuOperatorMetrics {
    // Execution metrics
    Counter gpuOperationsExecuted;
    Timer gpuExecutionTime;
    Timer cpuToGpuTransferTime;
    Timer gpuToCpuTransferTime;

    // Memory metrics
    Gauge gpuMemoryUsed;
    Gauge gpuMemoryPeak;
    Counter gpuMemoryAllocations;

    // Batch metrics
    Histogram batchSizeRows;
    Histogram batchSizeBytes;
    Counter batchesExecuted;
}
```

### 9.2 Query Statistics

Include GPU stats in query completion event:

```json
{
  "queryId": "20260327_123456_00001_abcde",
  "gpuStats": {
    "enabled": true,
    "operationsExecuted": 5,
    "totalGpuTime": "1.234s",
    "totalTransferTime": "0.123s",
    "peakGpuMemory": "2.5GB",
    "batchesProcessed": 42
  }
}
```

### 9.3 EXPLAIN Output

Show GPU operators in EXPLAIN plan:

```
Fragment 1 [HASH]
    GpuOperator[operations=3, estimatedGpuMemory=500MB]
        Filter: x > 100
        Project: x, y, z
        Aggregate: sum(z) GROUP BY x
    └─ TableScan: tpch.lineitem
```

### 9.4 Logging

Structured logging for GPU operations:

```java
log.debug("GPU operation started: operation=%s, inputRows=%d, estimatedGpuMemory=%s",
          operation.getClass().

getSimpleName(),
    inputPage.

getPositionCount(),

estimatedMemory);

        log.

debug("GPU operation completed: operation=%s, outputRows=%d, gpuTime=%dms, transferTime=%dms",
      operation.getClass().

getSimpleName(),
    outputPage.

getPositionCount(),

gpuTimeMs,
transferTimeMs);
```

---

## 10. Testing Strategy

### 10.1 Unit Tests

```java
// Test Page ↔ cuDF conversion
@Test
public void testPageToCudfConversion() {
    Page page = createTestPage();
    Table table = converter.convert(page);
    Page result = converter.convert(table);
    assertPagesEqual(page, result);
}

// Test GPU operations
@Test
public void testGpuFilter() {
    GpuPage input = createGpuPage();
    GpuFilterOperation filter = new GpuFilterOperation(predicate);
    GpuPage output = filter.execute(input, context);
    // Verify filter correctness
}
```

### 10.2 Integration Tests

```java

@Test
public void testGpuOperatorE2E() {
    // Create query with GPU-acceleratable pattern
    String sql = "SELECT sum(x) FROM test WHERE y > 100";

    // Execute with GPU enabled
    MaterializedResult result = executeWithGpu(sql);

    // Execute with GPU disabled (CPU reference)
    MaterializedResult expected = executeWithoutGpu(sql);

    // Results must match
    assertEquals(expected, result);
}
```

### 10.3 Correctness Testing

**Property-based testing:**

- Generate random data and predicates
- Execute same query on CPU and GPU
- Assert results are identical

**Edge cases:**

- Null values
- Empty pages
- Single-row pages
- Large pages (>1M rows)
- Mixed data types
- Dictionary-encoded blocks (future)

### 10.4 Performance Benchmarking

```java

@Benchmark
public void benchmarkGpuVsCpu() {
    // TPC-H queries with varying data sizes
    // Measure:
    // - Total query time
    // - GPU execution time
    // - Transfer overhead
    // - Memory usage
}
```

---

## 11. Implementation Phases

### Phase 1: Foundation (4-6 weeks)

**Goal:** Basic infrastructure and proof of concept

**Progress:** 🚧 In Progress (Updated 2026-04-02)

**Completed:**
- [x] **GpuPage abstraction** - Data model with `Column[]` and `Mask`
- [x] **Column sealed interface** - Three-level memory hierarchy (Blocks, HostMemory, DeviceMemory)
- [x] **Mask sealed interface** - Lazy filtering with `All` and `RetainedPositions`
- [x] **GpuOperation interface** - Pull model with `Result` sealed interface
- [x] **GpuOperator basic implementation** - Implements `Operator`, pulls from operations
- [x] **GpuScore enum** - POTENTIAL/PREFERRED scoring
- [x] **PhysicalOperation extensions** - Added `gpuPipelineTail` and `gpuPipelineScore` fields
- [x] **Expression compilation** - `GpuExpressionCompiler` class with scoring heuristics
- [x] **GpuExpression interface** - Low-level expression evaluation on GPU
- [x] **CompiledExpression record** - Bundles expression + input channels + score
- [x] **Incremental weaving pattern** - Design for building GPU pipeline in `visitXXX()` methods
- [x] **GpuFilterOperation** - Filters rows using boolean mask and cuDF Table.filter()
- [x] **GpuProjectOperation** - Projects columns with PassThrough and Gpu evaluation modes

**In Progress:**
- [ ] **GpuOperatorFactory** - Accumulates operations during planning, creates GpuOperator at runtime
- [ ] **visitScanFilterAndProject integration** - Wire up expression compilation in LocalExecutionPlanner
- [ ] **cuDF code generation** - Implement actual cuDF compilation for functions/operators (only InputReference works)

**TODO:**
- [ ] Page ↔ cuDF conversion for primitive types (INT, BIGINT, DOUBLE, VARCHAR)
- [ ] Batch accumulation logic (row/memory threshold) - GpuSourceOperation
- [ ] GPU memory management integration
- [ ] Configuration framework (gpu.enabled, session property)
- [ ] Basic metrics and logging
- [ ] Unit tests for conversion and filter
- [ ] End-to-end: Simple filter query running on GPU

**Current Milestone:**
- Core data structures: ✅ Complete
- Planning integration: ✅ Complete (design + PhysicalOperation changes)
- Expression compilation layer: ✅ Interfaces + stub (need cuDF implementation)
- Operation implementation: ⏳ Next step

**Next Steps:**
1. Implement `GpuOperatorFactory` (accumulates operations, creates `GpuOperator`)
2. Integrate expression compilation into `visitScanFilterAndProject`
3. Implement actual cuDF code generation in `GpuExpressionCompiler` (functions, operators, constants)
4. Implement Page ↔ cuDF conversion
5. Add support for CPU columns (Column.Blocks) in GPU operations
6. Implement `GpuSourceOperation` for batching

**Deliverable:** Working GPU filter + project on simple integer/string expressions

**Success Criteria:**

```sql
-- This query uses GPU operator
SET SESSION enable_gpu_acceleration = true;
SELECT *
FROM lineitem
WHERE l_quantity > 10;
```

### Phase 2: Core Operations (6-8 weeks)

**Goal:** Expand operation coverage

- [ ] Additional GPU operations:
    - [ ] Project (column selection, simple expressions)
    - [ ] Hash Aggregation (COUNT, SUM, MIN, MAX, AVG)
    - [ ] Simple string operations (LIKE, substring)
- [ ] Multi-operation pipelines (filter + project, filter + aggregate)
- [ ] LocalExecutionPlanner pattern matching for operator chains
- [ ] Support for more data types (DATE, TIMESTAMP, DECIMAL)
- [ ] Comprehensive integration tests
- [ ] Performance benchmarking framework
- [ ] EXPLAIN output showing GPU operators

**Deliverable:** Filter + Project + Aggregate on GPU

**Success Criteria:**

```sql
-- This query uses GPU operator with 3 operations
SELECT l_orderkey, sum(l_quantity)
FROM lineitem
WHERE l_shipdate > DATE '1995-01-01'
GROUP BY l_orderkey;
```

### Phase 3: Advanced Features (6-8 weeks)

**Goal:** Production-ready features

- [ ] Hash joins on GPU
- [ ] Window functions
- [ ] Complex expressions (CASE, CAST, arithmetic)
- [ ] Better memory management (explicit spilling)
- [ ] Query statistics and observability
- [ ] Performance optimization (reduce transfers, pipeline fusion)
- [ ] TPC-H benchmark suite
- [ ] Documentation

**Deliverable:** Production-ready GPU acceleration for core operations

**Success Criteria:**

- TPC-H Q1, Q6 run faster on GPU
- Comprehensive metrics available
- Documented performance characteristics

### Phase 4: Production Hardening (4-6 weeks)

**Goal:** Deployment readiness

- [ ] Multi-GPU support
- [ ] Stability testing (stress tests, memory leak detection)
- [ ] Performance regression testing
- [ ] Admin documentation (deployment, tuning)
- [ ] Cost-based optimizer integration (GPU vs CPU decision)
- [ ] Beta testing with real workloads

**Deliverable:** Production deployment

---

## 12. Key Design Decisions

### 12.1 Why Single Uber Operator?

**Alternative: Per-operation GPU operators (FilterGpuOperator, ProjectGpuOperator, etc.)**

| Aspect      | Uber Operator   | Per-Operation     |
|-------------|-----------------|-------------------|
| Complexity  | Higher initial  | Lower initial     |
| Performance | Better (fusion) | Worse (transfers) |
| Flexibility | Less flexible   | More flexible     |
| Testing     | Harder          | Easier            |

**Decision:** Uber operator for performance - GPU transfer overhead is expensive, need operation fusion.

### 12.2 Why No CPU Fallback?

**Alternative: Automatic fallback to CPU on GPU failure**

**Rationale:**

- Fallback doubles code complexity (need CPU implementation always)
- Makes debugging harder (which path failed?)
- User enabled GPU explicitly, should get GPU or know it failed
- Can add fallback later if needed (not a one-way door)

### 12.3 Why Lazy GPU Transfer and Masks?

**Alternative: Transfer all blocks to GPU eagerly and materialize filters immediately**

**Rationale for Lazy Transfer:**

- GPU memory limited (8-80GB)
- Many queries don't use all columns
- Transfer is expensive (PCIe bandwidth ~32GB/s)
- Example: Filter on column A from 100-column table
    - Lazy: Transfer only column A (1GB)
    - Eager: Transfer all 100 columns (100GB) - won't fit!

**Rationale for Masks:**

- Avoid copying filtered data prematurely
- Compose multiple filters without materialization
- Defer materialization decision to when operation requires it
- Example: Two sequential filters on 1M row table
    - Without masks: 
        - Filter 1 materializes → copies 500K rows
        - Filter 2 materializes → copies 100K rows
        - Total: 600K rows copied
    - With masks:
        - Filter 1 creates mask [500K positions]
        - Filter 2 composes mask [100K positions]
        - Materialize once → copies 100K rows
        - Total: 100K rows copied (6x less!)
- Enables efficient filter pushdown patterns
- Reduces GPU memory pressure during filtering

### 12.4 Memory Lifecycle: GpuPage Ownership

**Rationale:**

- GpuPage owns blocks, uses reference counting
- Blocks can be shared across GpuPages (refcount++)
- Natural fit with cuDF's ColumnVector lifecycle
- Prevents premature GPU memory free
- Similar to Spark RAPIDS approach

---

## 13. Future Research Topics

### 13.1 Dictionary-Encoded Blocks

**Problem:** Trino heavily uses dictionary encoding for low-cardinality columns

- Example: `DictionaryBlock` with 1M rows, 100 unique values
- GPU transfer: Send dictionary + indices, or materialize?

**Options:**

1. **Materialize before GPU**: Simple but wastes GPU memory
2. **Preserve encoding**: Transfer dictionary + indices separately
3. **Hybrid**: Materialize if small, preserve if large

**TODO:** Research and prototype each approach, measure performance

### 13.2 Complex/Nested Types

**Problem:** cuDF nested type support different from Trino

- Trino: ARRAY, MAP, ROW with arbitrary nesting
- cuDF: LIST, STRUCT with some limitations

**TODO:**

- Map Trino nested types to cuDF representation
- Handle nested operations (e.g., array subscript, map lookup)
- May require CPU fallback for some operations

### 13.3 Adaptive Batch Sizing

**Problem:** Fixed batch size suboptimal

- Small batches: GPU underutilized
- Large batches: High latency, memory pressure

**Options:**

- Adaptive based on GPU utilization
- Per-query tuning based on data characteristics
- Feedback loop from execution stats

**TODO:** Implement adaptive batching in Phase 3+

### 13.4 Cost-Based GPU Selection

**Problem:** Not all queries benefit from GPU

- Small queries: Transfer overhead > compute savings
- I/O-bound queries: GPU doesn't help

**Solution:** Extend Trino cost-based optimizer

- Estimate GPU vs CPU cost
- Consider data size, operation types, GPU availability
- Make CPU/GPU decision at planning time

**TODO:** Phase 4 - integrate with CBO

### 13.5 Zero-Copy Optimizations

**Problem:** CPU ↔ GPU transfer expensive

**Opportunities:**

- Pinned memory for faster transfers
- GPU-Direct Storage (read Parquet directly to GPU)
- Keep data in GPU across operators (already planned)
- GPU-to-GPU transfers in distributed queries (GPUDirect RDMA)

**TODO:** Measure transfer bottlenecks, optimize hot paths

---

## 14. Success Metrics

### 14.1 Functional Metrics

- [ ] All primitive types converting correctly
- [ ] Core operations (filter, project, aggregate) working
- [ ] 100% correctness: GPU results match CPU on all test queries
- [ ] No memory leaks in 24-hour stress test
- [ ] GPU operator in EXPLAIN output

### 14.2 Performance Metrics

**Target Speedups (vs CPU baseline):**

- TPC-H Q1: 3-5x faster
- TPC-H Q6: 5-10x faster
- Large aggregation queries: 5-15x faster
- Large join queries: 10-20x faster

**Overhead Acceptable:**

- Transfer time < 20% of total GPU execution time
- Memory overhead < 2x (GPU + CPU copies)

### 14.3 Production Readiness

- [ ] Configuration documented
- [ ] Runbook for GPU node setup
- [ ] Monitoring dashboard with GPU metrics
- [ ] Performance regression tests in CI
- [ ] Beta deployment handling production traffic

---

## 15. Dependencies and Prerequisites

### 15.1 External Dependencies

- **NVIDIA RAPIDS cuDF**: Version 24.10+
    - Maven: `ai.rapids:cudf:<version>`
    - Self-contained JAR with native libraries
- **CUDA Runtime**: 11.0+ (installed on nodes)
- **NVIDIA GPU Drivers**: Compatible with CUDA version
- **Hardware**: NVIDIA GPU with compute capability 6.0+ (Pascal or newer)

### 15.2 Trino Codebase Integration Points

| Component              | Changes Required                 | Complexity |
|------------------------|----------------------------------|------------|
| LocalExecutionPlanner  | Add GPU operator injection logic | High       |
| Operator interface     | Add GpuOperator extension        | Low        |
| MemoryManager          | Report GPU memory usage          | Medium     |
| SessionPropertyManager | Add enable_gpu_acceleration      | Low        |
| ConfigManager          | Add GpuConfig                    | Low        |
| MetricRegistry         | Add GPU metrics                  | Low        |
| ExplainVisitor         | Show GPU operators               | Low        |

### 15.3 Development Environment

- Linux development machine with NVIDIA GPU
- CUDA 11.0+ installed
- NVIDIA drivers installed and working
- Maven 3.8+
- Java 17+

---

## 16. Risks and Mitigations

| Risk                                              | Impact | Likelihood | Mitigation                                                  |
|---------------------------------------------------|--------|------------|-------------------------------------------------------------|
| cuDF API instability                              | High   | Medium     | Pin to stable cuDF version, test upgrades carefully         |
| GPU memory exhaustion                             | High   | High       | UVM for spilling, memory limits, monitoring                 |
| Poor performance on small queries                 | Medium | High       | Document use cases, consider auto-disable for small queries |
| Dictionary encoding complexity                    | High   | Medium     | Research early (Phase 1), may require CPU fallback          |
| Integration complexity with LocalExecutionPlanner | High   | Medium     | Prototype early, iterate on design                          |
| CUDA/driver issues in production                  | High   | Low        | Good error handling, logging, runbooks                      |
| Limited GPU hardware in testing                   | Medium | Medium     | Cloud GPU instances for testing, CI with GPU                |

---

## 17. Open Questions

1. **Pinned memory allocation**: Should we pre-allocate pinned memory pool for faster transfers?
2. **Multi-node GPU**: How to optimize distributed queries with GPUs on multiple nodes?
3. **GPU scheduling**: If multiple queries want GPU, how to schedule (FIFO, fair-share)?
4. **Partial GPU acceleration**: Should we support hybrid queries (some operators GPU, some CPU)?
5. **Catalog hints**: Should connectors provide hints about GPU-friendliness of tables?

---

## 18. References

### Prior Research & Prototypes

**Research Documentation:**

- **GPU Acceleration Research** ([gpu-acceleration-research.md](gpu-acceleration-research.md))
  - Comprehensive analysis of 20+ data processing systems with GPU acceleration
  - Key finding: Velox has experimental cuDF integration
  - RAPIDS cuDF emerging as standard approach
  - Performance benchmarks and architecture patterns
  - Strategic recommendation to proceed with GPU acceleration

**Trino GPU Acceleration Prototypes:**

- **Piotr's cuDF Research**: https://github.com/starburstdata/cork/commits/findepi/cudf
  - Initial cuDF integration exploration
  - Data structure prototypes
  - Conversion patterns
  
- **Martin's cuDF Research**: https://github.com/starburstdata/cork/commits/martin/cudf
  - Alternative integration approaches
  - Performance experiments
  - Architecture patterns

These prototypes and research provide valuable insights and learnings that informed the current implementation design.

### Documentation

- [NVIDIA RAPIDS cuDF](https://docs.rapids.ai/api/cudf/stable/)
- [Spark RAPIDS Architecture](https://nvidia.github.io/spark-rapids/)
- [Spark RAPIDS JNI for Starburst](Spark-RAPIDS-JNI-for-Starburst.pdf) - Internal document

### Code References

- Spark RAPIDS Plugin: https://github.com/NVIDIA/spark-rapids
- cuDF Java: https://github.com/rapidsai/cudf/tree/main/java
- Velox cuDF integration: https://github.com/facebookincubator/velox/tree/main/velox/experimental/cudf

### Trino Codebase

- LocalExecutionPlanner: `core/trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`
- Operator: `core/trino-main/src/main/java/io/trino/operator/Operator.java`
- Page: `core/trino-spi/src/main/java/io/trino/spi/Page.java`
- Block: `core/trino-spi/src/main/java/io/trino/spi/block/Block.java`

---

## Appendix A: Code Structure

Package structure (updated 2026-04-02):

```
core/trino-main/src/main/java/io/trino/operator/gpu/
├── GpuOperation.java          ✅ IMPLEMENTED - Pull model interface with nested Result
├── GpuOperator.java           ✅ IMPLEMENTED - Operator implementation (basic)
├── GpuPage.java               ✅ IMPLEMENTED - Data model
├── Mask.java                  ✅ IMPLEMENTED - Sealed interface (All, RetainedPositions)
├── Column.java                ✅ IMPLEMENTED - Sealed interface (Blocks, HostMemory, DeviceMemory)
├── GpuScore.java              ✅ IMPLEMENTED - POTENTIAL/PREFERRED enum
├── Borrow.java                ✅ IMPLEMENTED - Ownership annotation (borrowed reference)
├── Move.java                  ✅ IMPLEMENTED - Ownership annotation (ownership transfer)
├── GpuConfig.java             📝 TODO
├── GpuOperatorContext.java    📝 TODO
├── GpuOperatorFactory.java              📝 TODO - Accumulates operations during planning
├── GpuFilterOperation.java              ✅ IMPLEMENTED - Filters using boolean mask
├── GpuProjectOperation.java             ✅ IMPLEMENTED - Projects with PassThrough/Gpu modes
├── borrow/                              📁 NEW - Ownership tracking annotations
│   ├── Borrow.java                      ✅ IMPLEMENTED - Marks borrowed references
│   ├── Move.java                        ✅ IMPLEMENTED - Marks ownership transfer
│   └── Owned.java                       ✅ IMPLEMENTED - Marks owned resources
├── expression/                          📁 NEW - Expression compilation layer
│   ├── GpuExpressionCompiler.java       ✅ IMPLEMENTED - Compiler with visitor pattern
│   ├── GpuExpression.java               ✅ IMPLEMENTED - Expression evaluation interface
│   ├── CompiledExpression.java          ✅ IMPLEMENTED - Compilation result record
│   └── InputChannels.java               ✅ IMPLEMENTED - Tracks required input channels
├── memory/
│   ├── GpuMemoryManager.java            📝 TODO
│   └── DeviceMemoryBuffer.java          📝 TODO
├── operation/
│   ├── GpuSourceOperation.java          📝 TODO - Batching input Pages
│   ├── GpuAggregationOperation.java     📝 TODO
│   └── GpuJoinOperation.java            📝 TODO
├── conversion/
│   ├── PageToCudfConverter.java       📝 TODO
│   ├── CudfToPageConverter.java       📝 TODO
│   ├── TypeMapping.java               📝 TODO
│   └── MaskMaterializer.java          📝 TODO
└── planner/
    └── GpuPlanRewriter.java           📝 TODO - May not be needed (inline in visitXXX)

core/trino-main/src/main/java/io/trino/sql/planner/
└── LocalExecutionPlanner.java
    └── PhysicalOperation            ✅ IMPLEMENTED - Extended with GPU fields
        ├── gpuPipelineTail          ✅ Optional<List<OperatorFactory>>
        └── gpuPipelineScore         ✅ Optional<GpuScore>
```

**Implemented Files (Phase 1 - Updated 2026-04-02):**

**Core Data Structures:**
- **`GpuOperation.java`**: Interface with `Result` sealed interface (Blocked, Finished, Yielded, Data)
- **`GpuOperator.java`**: Basic `Operator` implementation, pulls from operations (conversion TODO)
- **`GpuPage.java`**: Data model with `positionCount`, `Column[]`, `Mask`
- **`Mask.java`**: Sealed interface with `All()` record and `RetainedPositions` class
- **`Column.java`**: Sealed interface with three implementations:
  - `Blocks(int, List<Block>)` - CPU memory
  - `HostMemory(HostColumnVector)` - Pinned CPU memory
  - `DeviceMemory(ColumnVector)` - GPU memory

**Ownership Annotations:**
- **`@Borrow`**: Marks borrowed references (no ownership transfer)
- **`@Move`**: Marks ownership transfer
- Required on ALL methods passing ColumnVector, HostColumnVector, or GpuPage
- Source-only annotations (`@Retention(SOURCE)`)
- Applicable to parameters and return values

**Planning Integration:**
- **`GpuScore.java`**: Public enum (POTENTIAL, PREFERRED) for scoring GPU benefit
- **`PhysicalOperation` extensions**: Added `gpuPipelineTail` and `gpuPipelineScore` fields
  - Both present or both absent (validated in constructor)
  - Enables parallel CPU/GPU pipeline building

**Expression Compilation Layer:**
- **`GpuExpressionCompiler.java`**: Concrete class for compiling RowExpressions to GPU (cuDF-based)
  - `compileExpression(RowExpression)` → `Optional<CompiledExpression>`
  - `CompiledExpression` record: bundles `GpuExpression` + `InputChannels` + `GpuScore`
  - Uses visitor pattern to traverse RowExpression tree
- **`GpuExpression.java`**: Interface for low-level expression evaluation
  - `evaluate(List<@Borrow ColumnVector>)` → `@Move ColumnVector`
  - Operates directly on cuDF ColumnVectors
- **`InputChannels.java`**: Tracks which input channels an expression requires
  - Used by operations to extract correct columns from GpuPage
  - Scoring heuristics: LIKE/regex → PREFERRED, others → POTENTIAL
  - Currently returns `Optional.empty()` (cuDF code generation not yet implemented)
  - Uses visitor pattern to walk RowExpression tree

**Key Design Decisions Implemented:**

1. **Parallel pipelines**: `PhysicalOperation` maintains both CPU and GPU pipelines
2. **Incremental weaving**: Each `visitXXX()` extends GPU pipeline by adding to `GpuOperatorFactory`
3. **Score propagation**: PREFERRED score "wins" and upgrades entire pipeline
4. **Bundled compilation results**: Score determined at compile-time, bundled with evaluator
5. **Mask as interface**: `All` sentinel (no `Optional`)
6. **Three-level memory hierarchy**: Blocks → HostMemory → DeviceMemory for efficient transfers
7. **Pull-based execution**: Sealed `Result` interface for state management
8. **AutoCloseable**: All data structures implement for resource management
9. **Expression compilation layer**: Clear separation between compilation (planning) and evaluation (execution)
10. **Ownership annotations**: `@Borrow` and `@Move` required on ALL GPU resource passing to prevent memory leaks

---

**END OF DOCUMENT**
