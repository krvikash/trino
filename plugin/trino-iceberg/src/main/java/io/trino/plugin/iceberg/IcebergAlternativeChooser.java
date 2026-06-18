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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import org.apache.iceberg.SortOrder;

import java.util.List;

public class IcebergAlternativeChooser
        implements ConnectorAlternativeChooser
{
    @Override
    public Choice chooseAlternative(
            ConnectorSession session,
            ConnectorSplit split,
            List<ConnectorTableHandle> alternatives)
    {
        int splitSortOrderId = ((IcebergSplit) split).sortOrderId();
        if (splitSortOrderId != SortOrder.unsorted().orderId()) {
            for (int i = 1; i < alternatives.size(); i++) {
                IcebergTableHandle tableHandle = (IcebergTableHandle) alternatives.get(i);
                if (tableHandle.getSortOrderId().isPresent()
                        && tableHandle.getSortOrderId().getAsInt() == splitSortOrderId) {
                    return new Choice(i);
                }
            }
        }

        return new Choice(0);
    }
}
