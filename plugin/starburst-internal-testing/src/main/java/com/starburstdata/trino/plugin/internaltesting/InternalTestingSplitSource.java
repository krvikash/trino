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
package com.starburstdata.trino.plugin.internaltesting;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InternalTestingSplitSource
        implements ConnectorSplitSource
{
    @Override
    public void close()
    {
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        List<ConnectorSplit> splits = ImmutableList.of(InternalTestingSplit.INSTANCE);
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, false));
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }
}
