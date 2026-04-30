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
package io.trino.plugin.base.gpu;

import io.trino.annotation.NotThreadSafe;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.gpu.ClosingOnce.closeUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * A mutable reference to {@link AutoCloseable} resource. This class ensures close idempotency.
 *
 * @see ClosingOnce if you do not need reference mutability
 */
@NotThreadSafe
public final class ClosingRef<T extends AutoCloseable>
        implements RuntimeCloseable
{
    public static <T extends AutoCloseable> @Move ClosingRef<T> empty()
    {
        return new ClosingRef<>(null);
    }

    public static <T extends AutoCloseable> @Move ClosingRef<T> own(@Move T value)
    {
        return new ClosingRef<>(requireNonNull(value, "value is null"));
    }

    @Own
    @Nullable
    private T value;

    private ClosingRef(@Move @Nullable T value)
    {
        this.value = value;
    }

    public @Borrow T borrow()
    {
        checkState(value != null, "No value");
        return value;
    }

    public @Move T take()
    {
        checkState(value != null, "No value");
        @Own T transferred = value;
        value = null;
        return transferred;
    }

    public void set(@Move T newValue)
    {
        requireNonNull(newValue, "newValue is null");
        if (value != null) {
            closeUnchecked(newValue);
            throw new IllegalStateException("Value already set");
        }
        value = newValue;
    }

    @Override
    public void close()
    {
        if (value != null) {
            try {
                closeUnchecked(value);
            }
            finally {
                value = null;
            }
        }
    }
}
