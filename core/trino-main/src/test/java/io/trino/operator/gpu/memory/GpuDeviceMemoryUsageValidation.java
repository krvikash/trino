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
package io.trino.operator.gpu.memory;

import ai.rapids.cudf.Rmm;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.TestingGpuOperationContext.ReservationListener;
import io.trino.spi.gpu.RuntimeCloseable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class GpuDeviceMemoryUsageValidation
        implements RuntimeCloseable
{
    public static GpuDeviceMemoryUsageValidation createAndRegister(TestingGpuOperationContext gpuOperationContext, long marginBytes)
    {
        Rmm.resetScopedMaximumBytesAllocated(Rmm.getTotalBytesAllocated());

        GpuDeviceMemoryUsageValidation validation = new GpuDeviceMemoryUsageValidation(gpuOperationContext, marginBytes);
        gpuOperationContext.setGpuDeviceMemoryReservationListener(validation.reservationListener);
        return validation;
    }

    private final long marginBytes;
    private final TestingGpuOperationContext gpuOperationContext;
    private final ReservationListener reservationListener = this::onReservationChange;

    @GuardedBy("this")
    private long reservation;
    @GuardedBy("this")
    private Exception reservee = new Exception("no-one");
    private ThreadLocal<Boolean> exemptValidation = new ThreadLocal<>()
    {
        @Override
        protected Boolean initialValue()
        {
            return false;
        }
    };
    @GuardedBy("this")
    private final List<RuntimeException> errors = new ArrayList<>();

    private GpuDeviceMemoryUsageValidation(TestingGpuOperationContext gpuOperationContext, long marginBytes)
    {
        this.gpuOperationContext = requireNonNull(gpuOperationContext, "gpuOperationContext is null");
        checkArgument(marginBytes >= 0);
        this.marginBytes = marginBytes;
    }

    @Override
    public synchronized void close()
    {
        gpuOperationContext.removeGpuDeviceMemoryReservationListener(reservationListener);
        finishValidation();
    }

    public void withoutValidation(Runnable action)
    {
        // So it's re-entrant
        boolean previousExempt = exemptValidation.get();
        try {
            exemptValidation.set(true);
            action.run();
        }
        finally {
            exemptValidation.set(previousExempt);
        }
    }

    private synchronized void onReservationChange(long newReservation, long delta)
    {
        verify(reservation + delta == newReservation,
                "Inconsistent state: reservation %s + delta %s != new reservation %s",
                reservation,
                delta,
                newReservation);

        long currentAllocation = Rmm.getTotalBytesAllocated();
        long peakBetweenReservations = Rmm.getScopedMaximumBytesAllocated();
        verify(peakBetweenReservations >= currentAllocation,
                "Sanity check: peak should be no less than current allocation: %s, %s",
                peakBetweenReservations,
                currentAllocation);

        if (!exemptValidation.get()) {
            // Allocation should not exceed reservation
            check(peakBetweenReservations <= reservation + marginBytes,
                    "Actual GPU device memory usage peak %s exceeded previous reservation of %s by %s (%s %%), new reservation is %s, delta %s",
                    peakBetweenReservations,
                    reservation,
                    peakBetweenReservations - reservation,
                    (peakBetweenReservations - reservation) * 100. / reservation,
                    newReservation,
                    delta);

            // Reservation should not have been overshot
            check(peakBetweenReservations >= reservation * 0.8 - marginBytes,
                    "Actual GPU device memory usage peak %s is significantly lower than previous reservation %s by %s %%",
                    peakBetweenReservations,
                    reservation,
                    (reservation - peakBetweenReservations) * 100. / reservation);

            // Allocation should not exceed reservation
            check(currentAllocation <= newReservation + marginBytes,
                    "Current GPU device memory usage %s is greater than new reservation %s, delta %s",
                    currentAllocation,
                    newReservation,
                    delta);
        }

        reservation = newReservation;
        reservee = new Exception("last reservation source");
        Rmm.resetScopedMaximumBytesAllocated(currentAllocation);
    }

    private synchronized void finishValidation()
    {
        check(reservation == 0, "Reservation at finish is non-zero: %s", reservation);
        onReservationChange(0, -reservation);

        if (!errors.isEmpty()) {
            AssertionError fail = new AssertionError("There are %s errors recorded, first few being shown".formatted(errors.size()));
            errors.stream().limit(10).forEach(fail::addSuppressed);
            throw fail;
        }
    }

    @GuardedBy("this")
    @FormatMethod
    private void check(boolean condition, String format, Object... params)
    {
        if (!condition) {
            IllegalStateException exception = new IllegalStateException(format.formatted(params));
            exception.addSuppressed(reservee);
            errors.add(exception);
        }
    }
}
