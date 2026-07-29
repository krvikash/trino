/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.getSubqueryCacheDataReductionThreshold;
import static java.util.Objects.requireNonNull;

public class CacheDataOperator
        implements Operator
{
    public static final int MIN_PROCESSED_POSITIONS = 16_384;
    public static final int MIN_PROCESSED_BYTES = 1024 * 1024; // 1MB

    public static class CacheDataOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final long maxSplitSizeInBytes;

        public CacheDataOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                long maxSplitSizeInBytes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.maxSplitSizeInBytes = maxSplitSizeInBytes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkArgument(driverContext.getCacheDriverContext().isPresent(), "cacheDriverContext is empty");
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, CacheDataOperator.class.getSimpleName());
            return new CacheDataOperator(operatorContext, maxSplitSizeInBytes);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CacheDataOperatorFactory(operatorId, planNodeId, maxSplitSizeInBytes);
        }
    }

    private final OperatorContext operatorContext;
    private final CacheMetrics cacheMetrics;
    private final CacheStats cacheStats;
    private Metrics metrics;
    private final LocalMemoryContext memoryContext;
    private final CacheDriverContext cacheContext;
    private final long maxCacheSizeInBytes;

    @Nullable
    private ConnectorPageSink pageSink;
    @Nullable
    private Page page;
    private long cachedDataSize;
    private boolean finishing;

    private CacheDataOperator(OperatorContext operatorContext, long maxCacheSizeInBytes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.memoryContext = operatorContext.newLocalUserMemoryContext(CacheDataOperator.class.getSimpleName());
        this.cacheContext = operatorContext.getDriverContext().getCacheDriverContext()
                .orElseThrow(() -> new IllegalArgumentException("Cache context is not present"));
        this.cacheMetrics = cacheContext.cacheMetrics();
        this.cacheStats = cacheContext.cacheStats();
        this.pageSink = cacheContext
                .pageSink()
                .orElseThrow(() -> new IllegalArgumentException("Cache page sink is not present"));
        memoryContext.setBytes(pageSink.getMemoryUsage());
        this.maxCacheSizeInBytes = maxCacheSizeInBytes;
        this.metrics = cacheContext.metrics();
        operatorContext.setLatestMetrics(metrics);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && page == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());
        this.page = page;

        if (pageSink == null) {
            // caching was aborted
            return;
        }

        checkState(pageSink.appendPage(page).isDone(), "appendPage future must be done");
        cachedDataSize += page.getSizeInBytes();
        memoryContext.setBytes(pageSink.getMemoryUsage());

        // If there is no space for a page in a cache, stop caching this split and abort pageSink
        if (pageSink.getMemoryUsage() > maxCacheSizeInBytes) {
            abort();
            metrics = metrics.mergeWith(new Metrics(ImmutableMap.of(
                    "Too big split", new LongCount(1))));
            operatorContext.setLatestMetrics(metrics);
            cacheMetrics.incrementTooBigSplitCount();
            cacheStats.recordTooBigSplit();
        }
        if (thresholdExceeded(page.getPositionCount())) {
            abort();
            metrics = metrics.mergeWith(new Metrics(ImmutableMap.of(
                    "Insufficient data reduction", new LongCount(1))));
            operatorContext.setLatestMetrics(metrics);
            cacheStats.recordInsufficientDataReduction();
        }
    }

    private boolean thresholdExceeded(long currentPagePositions)
    {
        if (operatorContext.getOperatorStats().getInputPositions() + currentPagePositions < MIN_PROCESSED_POSITIONS
                || cachedDataSize < MIN_PROCESSED_BYTES) {
            return false;
        }
        long sourceBytes = getSourceBytes();
        if (sourceBytes == 0) {
            return false;
        }
        double dataReductionRatio = cachedDataSize / (double) sourceBytes;
        return dataReductionRatio > getSubqueryCacheDataReductionThreshold(operatorContext.getSession());
    }

    @Override
    public Page getOutput()
    {
        Page page = this.page;
        this.page = null;
        return page;
    }

    @Override
    public void finish()
    {
        finishing = true;
        if (pageSink != null) {
            checkState(pageSink.finish().isDone(), "finish future must be done");
            pageSink = null;
            memoryContext.close();

            recordCost();
            recordPotentialGain();
            cacheMetrics.incrementSplitsCached();
            cacheMetrics.addSourceBytes(getSourceBytes());
            cacheMetrics.addInputCacheBytes(cachedDataSize);
            cacheStats.recordCacheData(cachedDataSize);
        }
    }

    private long getSourceBytes()
    {
        return operatorContext.getDriverContext().getOperatorContexts().getFirst().getOperatorStats().getInputDataSize().toBytes();
    }

    @Override
    public boolean isFinished()
    {
        return finishing && page == null;
    }

    @Override
    public void close()
            throws Exception
    {
        if (pageSink != null) {
            abort();
        }
    }

    private void abort()
    {
        requireNonNull(pageSink, "pageSink is null");
        pageSink.abort();
        pageSink = null;
        memoryContext.close();
        cacheMetrics.incrementSplitsNotCached();
        cacheMetrics.addSourceBytes(getSourceBytes());
        cacheMetrics.addInputCacheBytes(cachedDataSize);
        recordCost();
    }

    private void recordCost()
    {
        // the cost of adaptation is neglected
        long cpuNanos = -1 * operatorContext.getCpuNanos();
        cacheStats.safeUpdateSparedCpuTime(cpuNanos);

        metrics = metrics.mergeWith(new Metrics(ImmutableMap.of(
                "Spared CPU time (ns)", new LongCount(cpuNanos))));
        operatorContext.setLatestMetrics(metrics);
    }

    private void recordPotentialGain()
    {
        long cpuNanos = 0;
        for (OperatorContext currentContext : operatorContext.getDriverContext().getOperatorContexts()) {
            if ((currentContext.getOperatorId() == operatorContext.getOperatorId())) {
                cacheContext.recordPotentialGain(cpuNanos);
                break;
            }

            cpuNanos += currentContext.getCpuNanos();
        }
    }
}
