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
package io.trino.plugin.hive.parquet;

import ai.rapids.cudf.HostMemoryBuffer;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.gpu.RuntimeCloseable;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class Buffers
        implements RuntimeCloseable
{
    private List<@Own HostMemoryBuffer> buffers;

    public Buffers(@Move List<HostMemoryBuffer> buffers)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
    }

    public List<@Borrow HostMemoryBuffer> buffers()
    {
        checkState(buffers != null, "Already closed");
        return buffers;
    }

    @Override
    public void close()
    {
        if (buffers != null) {
            try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
                buffers.forEach(closer::register);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
                buffers = null;
            }
        }
    }
}
