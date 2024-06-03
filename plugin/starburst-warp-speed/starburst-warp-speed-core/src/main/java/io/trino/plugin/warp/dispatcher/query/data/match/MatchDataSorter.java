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
package io.trino.plugin.warp.dispatcher.query.data.match;

import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.warp.WarpErrorCode.WARP_PREDICATE_CACHE_ERROR;

public class MatchDataSorter
{
    private MatchDataSorter() {}

    public static List<MatchData> sortMatchDataByWarmupType(List<MatchData> matchDatas, QueryContext queryContext)
    {
        List<MatchData> result = new ArrayList<>(matchDatas);
        result.sort((matchData1, matchData2) -> {
            int value1 = getValue(matchData1, queryContext);
            int value2 = getValue(matchData2, queryContext);
            return value1 - value2;
        });
        return result;
    }

    public static int getValue(MatchData matchData, QueryContext queryContext)
    {
        int res;
        if (matchData instanceof LogicalMatchData logicalMatchData) {
            res = logicalMatchData.getOperator() == LogicalMatchData.Operator.OR ? IndexPriority.OR.ordinal() : 0;
        }
        else if (matchData instanceof QueryMatchData queryMatchData) {
            WarmUpType warmUpType = queryMatchData.getWarmUpElement().getWarmUpType();
            // We prefer to first match on columns which are not match-collected since match-collect requires more resources (CPU + memory))
            res = switch (warmUpType) {
                case WARM_UP_TYPE_DATA -> IndexPriority.DATA.ordinal();
                case WARM_UP_TYPE_BASIC -> MatchCollectUtils.canBeMatchForMatchCollect(queryMatchData, queryContext.getNativeQueryCollectDataList()) ?
                        IndexPriority.BASIC_WITH_COLLECT.ordinal() : IndexPriority.BASIC_WITHOUT_COLLECT.ordinal();
                case WARM_UP_TYPE_LUCENE -> IndexPriority.LUCENE.ordinal();
                default -> throw new TrinoException(WARP_PREDICATE_CACHE_ERROR, "got invalid warmUpType=" + warmUpType);
            };
        }
        else {
            throw new RuntimeException("unsupported sort for matchData type " + matchData);
        }
        return res;
    }

    private enum IndexPriority
    {
        DATA, // for index data collection
        BASIC_WITHOUT_COLLECT, // basic is preferred over lucene
        LUCENE,
        BASIC_WITH_COLLECT, // basic with collect is lowest since it requires collect operation which is costly
        OR
    }
}
