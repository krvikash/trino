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
package io.trino.plugin.warp.storage.read;

import java.lang.foreign.MemorySegment;

public class StorageCollectorCallBack
{
    private final TxArgs txArgs;
    private int collectStoreCurrSize;

    public StorageCollectorCallBack(TxArgs txArgs)
    {
        this.txArgs = txArgs;
        this.collectStoreCurrSize = 0;
    }

    // this method is used only as a StorageEngine callback from native code
    @SuppressWarnings("unused")
    void collectStoreStateCB(int size)
    {
        MemorySegment.copy(txArgs.collectStateBuff(), 0, MemorySegment.ofArray(txArgs.collectStoreBuff()), 0, size);
        collectStoreCurrSize = size;
    }

    // this method is used only as a StorageEngine callback from native code
    // returns the size of the state restored
    @SuppressWarnings("unused")
    int collectRestoreStateCB(int dummy)
    {
        if (collectStoreCurrSize > 0) {
            MemorySegment.copy(MemorySegment.ofArray(txArgs.collectStoreBuff()), 0, txArgs.collectStateBuff(), 0, collectStoreCurrSize);
        }
        return collectStoreCurrSize;
    }
}
