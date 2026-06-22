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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.Set;

public class SasSplitManager
        implements ConnectorSplitManager
{
    private final int splitCount;
    private final int minPagePerSplit;
    private static final Logger log = Logger.get(SasSplitManager.class);

    @Inject
    public SasSplitManager(SasConfig config)
    {
        this.splitCount = config.getSplitCount();
        this.minPagePerSplit = config.getMinPagePerSplit();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        SasTableHandle tableHandle = (SasTableHandle) connectorTableHandle;
        long pageCount = tableHandle.pageCount();

        int effectiveSplitCount = tableHandle.splits() > 0 ? tableHandle.splits() : splitCount;
        if (tableHandle.splits() > 0) {
            log.debug("Split count overridden by table handle: %s", effectiveSplitCount);
        }

        int pagesPerSplit = Math.max((int) pageCount / effectiveSplitCount, minPagePerSplit);

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        int start = 0;
        do {
            log.debug("SasConnector split page: %s, total: %s, start: %s, pagesPerSplit: %s", tableHandle.source(), pageCount, start, pagesPerSplit);
            splits.add(new SasSplit(tableHandle.source(), start, 0, pagesPerSplit));
            start += pagesPerSplit;
        }
        while (start < pageCount);

        return new FixedSplitSource(splits.build());
    }
}
