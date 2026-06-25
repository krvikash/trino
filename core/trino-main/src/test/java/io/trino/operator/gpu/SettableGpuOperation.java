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
package io.trino.operator.gpu;

import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;

import static com.google.common.base.Preconditions.checkState;

public class SettableGpuOperation
        implements GpuOperation
{
    private final ClosingRef<AllocatedMemory> pendingMemory;
    private final ClosingRef<GpuPage> pendingPage;
    private boolean noMoreInput;

    public SettableGpuOperation()
    {
        this.pendingMemory = ClosingRef.empty();
        this.pendingPage = ClosingRef.empty();
    }

    public boolean hasPending()
    {
        return !pendingPage.isEmpty();
    }

    public void setPending(@Move AllocatedMemory allocation, @Move GpuPage page)
    {
        checkState(!noMoreInput, "noMoreInput already set");
        pendingMemory.set(allocation);
        pendingPage.set(page);
    }

    public void noMoreInput()
    {
        noMoreInput = true;
    }

    public boolean isExhausted()
    {
        return !hasPending() && noMoreInput;
    }

    @Override
    public Result execute()
    {
        if (!pendingPage.isEmpty()) {
            return new Data(pendingMemory.take(), pendingPage.take());
        }
        if (noMoreInput) {
            return new Finished();
        }
        return new Yielded();
    }

    @Override
    public void close()
    {
        pendingMemory.close();
        pendingPage.close();
    }
}
