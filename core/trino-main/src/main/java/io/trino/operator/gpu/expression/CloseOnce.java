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
package io.trino.operator.gpu.expression;

import io.trino.annotation.NotThreadSafe;
import io.trino.operator.gpu.RuntimeCloseable;
import io.trino.operator.gpu.borrow.Borrow;
import io.trino.operator.gpu.borrow.Move;
import io.trino.operator.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public final class CloseOnce<T extends AutoCloseable>
        implements RuntimeCloseable
{
    public static <T extends AutoCloseable> @Move CloseOnce<T> own(@Move T value)
    {
        return new CloseOnce<>(value);
    }

    @Own
    @Nullable
    private T value;

    private CloseOnce(@Move T value)
    {
        this.value = requireNonNull(value, "value is null");
    }

    public @Borrow T value()
    {
        checkState(value != null, "Already closed");
        return value;
    }

    @Override
    public void close()
    {
        if (value != null) {
            try {
                value.close();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                value = null;
            }
        }
    }
}
