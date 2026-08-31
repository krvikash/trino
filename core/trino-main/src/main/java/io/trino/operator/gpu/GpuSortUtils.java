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

import ai.rapids.cudf.OrderByArg;
import io.trino.spi.connector.SortOrder;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class GpuSortUtils
{
    private GpuSortUtils() {}

    public static OrderByArg[] toOrderByArgs(int[] sortChannels, List<SortOrder> sortOrders)
    {
        checkArgument(sortChannels.length == sortOrders.size(), "sortChannels and sortOrders sizes do not match");
        OrderByArg[] args = new OrderByArg[sortChannels.length];
        for (int i = 0; i < sortChannels.length; i++) {
            SortOrder order = sortOrders.get(i);
            args[i] = order.isAscending()
                    ? OrderByArg.asc(sortChannels[i], order.isNullsFirst())
                    : OrderByArg.desc(sortChannels[i], !order.isNullsFirst());
        }
        return args;
    }
}
