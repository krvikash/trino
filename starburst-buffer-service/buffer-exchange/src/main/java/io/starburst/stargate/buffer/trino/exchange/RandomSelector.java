/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import javax.inject.Provider;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToDoubleFunction;
import java.util.random.RandomGenerator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// todo migrate to class from Airlift (PR in flight https://github.com/airlift/airlift/pull/1007)
public interface RandomSelector<T>
{
    T next();

    static <T> RandomSelector<T> weighted(Collection<T> collection, ToDoubleFunction<T> weightFunction, RandomGenerator random)
    {
        return new WeightedRandomSelector<T>(collection, weightFunction, () -> random);
    }

    static <T> RandomSelector<T> weighted(Collection<T> collection, ToDoubleFunction<T> weightFunction)
    {
        return new WeightedRandomSelector<T>(collection, weightFunction, ThreadLocalRandom::current);
    }

    class WeightedRandomSelector<T>
            implements RandomSelector<T>
    {
        private final Provider<RandomGenerator> randomProvider;
        private final Object[] objects;
        private final double[] weights;
        private final double totalWeight;

        private WeightedRandomSelector(Collection<T> collection, ToDoubleFunction<T> weightFunction, Provider<RandomGenerator> randomProvider)
        {
            this.randomProvider = requireNonNull(randomProvider, "randomProvider is null");

            int size = collection.size();
            checkArgument(size > 0, "empty collection");
            objects = new Object[size];
            weights = new double[size];

            double totalWeight = 0.0;
            int i = 0;
            for (T object : collection) {
                double weight = weightFunction.applyAsDouble(object);
                checkArgument(weight >= 0, "got negative weight for %s", object);
                objects[i] = object;
                weights[i] = weight;
                totalWeight += weight;
                i++;
            }
            this.totalWeight = totalWeight;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next()
        {
            double randomValue = randomProvider.get().nextDouble() * totalWeight;
            for (int i = 0; i < objects.length; i++) {
                if (weights[i] >= randomValue) {
                    return (T) objects[i];
                }
                randomValue -= weights[i];
            }
            // maybe there is slight chance to get here due to rounding errors in floating point arithmetic
            return (T) objects[objects.length - 1];
        }
    }
}
