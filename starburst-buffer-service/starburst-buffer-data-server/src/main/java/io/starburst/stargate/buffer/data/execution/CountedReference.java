/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CountedReference<T>
{
    private final T value;
    private final Consumer<T> destroyAction;
    private final AtomicInteger referenceCount = new AtomicInteger(1);

    private CountedReference(T value, Consumer<T> destroyAction)
    {
        this.value = requireNonNull(value, "value is null");
        this.destroyAction = requireNonNull(destroyAction, "destroyAction is null");
    }

    public static <T> Ref<T> create(Supplier<T> valueSupplier, Consumer<T> destroyAction)
    {
        T value = requireNonNull(valueSupplier, "valueSupplier is null").get();
        CountedReference<T> reference = new CountedReference<>(value, destroyAction);
        return new Ref<>(reference);
    }

    @ThreadSafe
    public static final class Ref<T>
    {
        private final CountedReference<T> countedReference;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private Ref(CountedReference<T> countedReference)
        {
            this.countedReference = countedReference;
        }

        public T get()
        {
            checkState(countedReference.referenceCount.get() > 0, "resource has been destroyed");
            return countedReference.value;
        }

        /**
         * Acquires an additional reference. The caller must ensure the reference is still live.
         * The precondition check is best-effort and not atomic with the increment.
         */
        public Ref<T> addReference()
        {
            checkState(countedReference.referenceCount.get() > 0, "cannot add reference after resource has been destroyed");
            countedReference.referenceCount.incrementAndGet();
            return new Ref<>(countedReference);
        }

        /**
         * Releases a reference. When the final reference is released, the destroy action will be invoked.
         * The caller must ensure the reference is still live and handle possible exceptions.
         */
        public void release()
        {
            checkState(released.compareAndSet(false, true), "reference already released");
            if (countedReference.referenceCount.decrementAndGet() == 0) {
                countedReference.destroyAction.accept(countedReference.value);
            }
        }
    }
}
