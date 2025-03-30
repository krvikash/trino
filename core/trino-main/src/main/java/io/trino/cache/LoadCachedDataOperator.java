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
import io.trino.metadata.Split;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LoadCachedDataOperator
        implements SourceOperator
{
    public static class LoadCachedDataOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;

        public LoadCachedDataOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LoadCachedDataOperator.class.getSimpleName());
            return new LoadCachedDataOperator(operatorContext, planNodeId);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LoadCachedDataOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final CacheStats cacheStats;
    private final Optional<Long> sparedCpuNanos;
    private final Metrics metrics;
    private final LocalMemoryContext memoryContext;

    @Nullable
    private ConnectorPageSource pageSource;

    private LoadCachedDataOperator(OperatorContext operatorContext, PlanNodeId planNodeId)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.memoryContext = operatorContext.newLocalUserMemoryContext(LoadCachedDataOperator.class.getSimpleName());
        CacheDriverContext cacheContext = operatorContext.getDriverContext().getCacheDriverContext()
                .orElseThrow(() -> new IllegalArgumentException("Cache context is not present"));
        this.cacheStats = cacheContext.cacheStats();
        this.sparedCpuNanos = cacheContext.getSparedCpu();
        this.pageSource = cacheContext
                .pageSource()
                .orElseThrow(() -> new IllegalArgumentException("Cache page sink is not present"));
        memoryContext.setBytes(pageSource.getMemoryUsage());
        this.metrics = cacheContext.metrics();
        operatorContext.setLatestMetrics(metrics);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public void addSplit(Split split)
    {
        // noop
    }

    @Override
    public void noMoreSplits()
    {
        // noop
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput()
    {
        if (pageSource == null) {
            return null;
        }

        Page page = pageSource.getNextPage();
        if (page == null) {
            return null;
        }

        cacheStats.recordReadFromCacheData(page.getSizeInBytes());
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        memoryContext.setBytes(pageSource.getMemoryUsage());
        return page.getLoadedPage();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource == null || pageSource.isFinished();
    }

    @Override
    public void close()
            throws Exception
    {
        finish();
    }

    @Override
    public void finish()
    {
        try {
            if (pageSource != null) {
                pageSource.close();
                operatorContext.setLatestConnectorMetrics(pageSource.getMetrics());
                pageSource = null;
                memoryContext.close();

                if (sparedCpuNanos.isEmpty()) {
                    cacheStats.getMissingStatForSparedCpuTime().update(1);
                    operatorContext.setLatestMetrics(metrics.mergeWith(new Metrics(ImmutableMap.of(
                            "Missing stat for spared CPU time", new LongCount(1)))));
                }
                else {
                    long cpuNanos = sparedCpuNanos.get() - operatorContext.getCpuNanos(); // reduce the cost of loading (the cost of adaptation is neglected)
                    cacheStats.safeUpdateSparedCpuTime(cpuNanos);
                    operatorContext.setLatestMetrics(metrics.mergeWith(new Metrics(ImmutableMap.of(
                            "Spared CPU time (ns)", new LongCount(cpuNanos)))));
                }
            }
        }
        catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
