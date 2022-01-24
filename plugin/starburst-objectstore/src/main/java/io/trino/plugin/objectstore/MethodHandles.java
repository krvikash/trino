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
package io.trino.plugin.objectstore;

import com.google.common.base.Throwables;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class MethodHandles
{
    private MethodHandles() {}

    public static <T> Optional<MethodHandle> translateArguments(MethodHandle methodHandle, Class<T> argumentType, Function<T, T> function)
    {
        int parameterCount = methodHandle.type().parameterList().size();
        int[] argumentPositions = IntStream.range(0, parameterCount)
                .filter(i -> methodHandle.type().parameterType(i) == argumentType)
                .toArray();

        if (argumentPositions.length == 0) {
            return Optional.empty();
        }

        MethodHandle spreader = methodHandle.asSpreader(Object[].class, parameterCount);
        Translate<?> proxy = new Translate<T>(argumentType, argumentPositions, function, spreader);
        MethodHandle proxyMethodHandle = Translate.INVOKE.bindTo(proxy)
                .asCollector(Object[].class, parameterCount)
                .asType(methodHandle.type());

        return Optional.of(proxyMethodHandle);
    }

    static class Translate<T>
    {
        private static final MethodHandle INVOKE;

        static {
            try {
                INVOKE = lookup().unreflect(Translate.class.getMethod("invoke", Object[].class));
            }
            catch (ReflectiveOperationException e) {
                throw new LinkageError(e.getMessage(), e);
            }
        }

        private final Class<T> argumentType;
        private final int[] argumentPositions;
        private final Function<T, T> function;
        private final MethodHandle delegate;

        public Translate(Class<T> argumentType, int[] argumentPositions, Function<T, T> function, MethodHandle delegate)
        {
            this.argumentType = requireNonNull(argumentType, "argumentType is null");
            this.argumentPositions = argumentPositions.clone();
            this.function = requireNonNull(function, "function is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        public Object invoke(Object[] arguments)
        {
            Object[] copy = arguments.clone();
            for (int position : argumentPositions) {
                copy[position] = function.apply(argumentType.cast(arguments[position]));
            }
            try {
                return delegate.invoke(copy);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
    }
}
