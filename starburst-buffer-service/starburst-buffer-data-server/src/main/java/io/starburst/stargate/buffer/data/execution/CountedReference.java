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

    public static <T> Handle<T> create(Supplier<T> valueSupplier, Consumer<T> destroyAction)
    {
        T value = requireNonNull(valueSupplier, "valueSupplier is null").get();
        CountedReference<T> reference = new CountedReference<>(value, destroyAction);
        return new Handle<>(reference, reference.createDestroyCallback());
    }

    public T get()
    {
        checkState(referenceCount.get() > 0, "resource has been destroyed");
        return value;
    }

    public Runnable addReference()
    {
        checkState(referenceCount.get() > 0, "cannot add reference after resource has been destroyed");
        referenceCount.incrementAndGet();
        return createDestroyCallback();
    }

    private Runnable createDestroyCallback()
    {
        return new Runnable()
        {
            private final AtomicBoolean released = new AtomicBoolean(false);

            @Override
            public void run()
            {
                checkState(released.compareAndSet(false, true), "reference already released");
                if (referenceCount.decrementAndGet() == 0) {
                    destroyAction.accept(value);
                }
            }
        };
    }

    public record Handle<T>(CountedReference<T> countedReference, Runnable destroyCallback)
    {
        public T get()
        {
            return countedReference.get();
        }

        public Runnable addReference()
        {
            return countedReference.addReference();
        }

        public void release()
        {
            destroyCallback.run();
        }
    }
}
