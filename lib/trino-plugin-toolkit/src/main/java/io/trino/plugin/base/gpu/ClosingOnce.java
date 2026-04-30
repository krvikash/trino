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
import static java.util.Objects.requireNonNull;

/**
 * A wrapper around {@link AutoCloseable} resource ensuring close idempotency.
 *
 * @see ClosingRef if you need a mutable reference
 */
@NotThreadSafe
public final class ClosingOnce<T extends AutoCloseable>
        implements RuntimeCloseable
{
    public static <T extends AutoCloseable> @Move ClosingOnce<T> own(@Move T value)
    {
        return new ClosingOnce<>(value);
    }

    @Own
    @Nullable
    private T value;

    private ClosingOnce(@Move T value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    public @Borrow T borrow()
    {
        checkState(value != null, "Already closed");
        return value;
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

    static void closeUnchecked(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
