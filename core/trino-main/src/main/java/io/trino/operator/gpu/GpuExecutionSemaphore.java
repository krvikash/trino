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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Process-wide semaphore limiting concurrent GPU work.
 */
public class GpuExecutionSemaphore
{
    public sealed interface AcquisitionResult {}

    /**
     * A permit was acquired; the caller must call {@link #release()} exactly once when done.
     */
    public record Granted()
            implements AcquisitionResult {}

    /**
     * No permit available; wait on {@code wakeup}, then call {@link #tryAcquire} again.
     */
    public record Denied(ListenableFuture<Void> wakeup)
            implements AcquisitionResult
    {
        public Denied
        {
            requireNonNull(wakeup, "wakeup is null");
        }
    }

    private final int maxPermits;
    @GuardedBy("this")
    private int availablePermits;

    @GuardedBy("this")
    private NonCancellableGpuSemaphoreFuture wakeup = NonCancellableGpuSemaphoreFuture.create();

    @Inject
    public GpuExecutionSemaphore(GpuConfig config)
    {
        this.maxPermits = config.getExecutionConcurrency();
        this.availablePermits = config.getExecutionConcurrency();
    }

    public synchronized AcquisitionResult tryAcquire()
    {
        if (availablePermits > 0) {
            availablePermits--;
            return new Granted();
        }
        return new Denied(wakeup);
    }

    public void release()
    {
        NonCancellableGpuSemaphoreFuture toWake = null;

        synchronized (this) {
            checkState(availablePermits >= 0, "availablePermits went negative in release: %s", availablePermits);
            checkState(availablePermits < maxPermits, "availablePermits would exceed maxPermits in release: availablePermits=%s maxPermits=%s", availablePermits, maxPermits);

            availablePermits++;

            if (availablePermits == 1) {
                toWake = wakeup;
                wakeup = NonCancellableGpuSemaphoreFuture.create();
            }
        }

        if (toWake != null) {
            toWake.set(null);
        }
    }

    private static class NonCancellableGpuSemaphoreFuture
            extends AbstractFuture<Void>
    {
        public static NonCancellableGpuSemaphoreFuture create()
        {
            return new NonCancellableGpuSemaphoreFuture();
        }

        @Override
        public boolean set(@Nullable Void value)
        {
            return super.set(value);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException("cancellation is not supported");
        }
    }
}
