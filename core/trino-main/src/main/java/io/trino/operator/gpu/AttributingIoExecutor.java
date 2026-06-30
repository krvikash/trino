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
package io.trino.operator.gpu;

import io.trino.operator.OperatorContext;
import io.trino.spi.gpu.IoExecutor;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a shared {@link IoExecutor} and attributes each task's CPU to a single operator, so that
 * reads offloaded off the driver thread still count toward the query's CPU usage.
 */
public final class AttributingIoExecutor
        implements IoExecutor
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final IoExecutor delegate;
    private final OperatorContext operatorContext;

    public AttributingIoExecutor(IoExecutor delegate, OperatorContext operatorContext)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task)
    {
        requireNonNull(task, "task is null");
        return delegate.submit(() -> {
            long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            try {
                return task.call();
            }
            finally {
                operatorContext.recordOffThreadCpu(max(0, THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart));
            }
        });
    }
}
