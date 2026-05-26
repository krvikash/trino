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

import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.gpu.RuntimeCloseable;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Like {@link com.google.common.io.Closer} but unchecked.
 */
public final class UncheckedCloser
        implements RuntimeCloseable
{
    public static UncheckedCloser create()
    {
        return new UncheckedCloser();
    }

    private final AutoCloseableCloser delegate = AutoCloseableCloser.create();

    private UncheckedCloser() {}

    public <C extends RuntimeCloseable> C register(C closeable)
    {
        return delegate.register(closeable);
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            // Unreachable
            throw new RuntimeException(e);
        }
    }
}
