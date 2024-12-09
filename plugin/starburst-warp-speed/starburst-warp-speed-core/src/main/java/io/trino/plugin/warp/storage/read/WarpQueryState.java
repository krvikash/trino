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

import java.util.Optional;

public class WarpQueryState
{
    private int numRecordsInCurPage;
    private int totalNumReadRecords;
    private Optional<StoreRowListResult> storeRowListResult;

    public WarpQueryState()
    {
        this.storeRowListResult = Optional.empty();
    }

    public int getNumRecordsInCurPage()
    {
        return numRecordsInCurPage;
    }

    public void setNumRecordsInCurPage(int numRecordsInCurPage)
    {
        this.numRecordsInCurPage = numRecordsInCurPage;
    }

    public void resetNumRecordsInCurPage()
    {
        this.numRecordsInCurPage = 0;
    }

    public int getTotalNumReadRecords()
    {
        return totalNumReadRecords;
    }

    public void addTotalNumReadRecords(int toAdd)
    {
        this.totalNumReadRecords += toAdd;
    }

    public Optional<StoreRowListResult> getStoreRowListResult()
    {
        return storeRowListResult;
    }

    public void setStoreRowListResult(Optional<StoreRowListResult> storeRowListResult)
    {
        this.storeRowListResult = storeRowListResult;
    }
}
