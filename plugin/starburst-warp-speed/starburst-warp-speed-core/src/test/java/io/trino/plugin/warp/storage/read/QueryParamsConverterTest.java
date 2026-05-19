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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.MatchData;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.PredicateBufferInfo;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryParamsConverterTest
{
    @Test
    public void testConvertMatchDataListStructure()
    {
        LuceneQueryMatchData luceneQueryMatchData0 = createLuceneQueryMatchData("lucene0", 0);
        LuceneQueryMatchData luceneQueryMatchData1 = createLuceneQueryMatchData("lucene1", 1);
        LuceneQueryMatchData luceneQueryMatchData2 = createLuceneQueryMatchData("lucene2", 2);

        MatchData matchData = new LogicalMatchData(
                LogicalMatchData.Operator.AND,
                List.of(luceneQueryMatchData0, new LogicalMatchData(LogicalMatchData.Operator.OR, List.of(luceneQueryMatchData1, luceneQueryMatchData2))));

        QueryContext queryContext = mock(QueryContext.class);
        when(queryContext.getMatchData()).thenReturn(Optional.of(matchData));
        when(queryContext.getNativeQueryCollectDataList()).thenReturn(ImmutableList.of());
        when(queryContext.getTotalRecords()).thenReturn(100);
        QueryParams queryParams = QueryParamsConverter.createQueryParams(
                new WorkerMemoryManager(new CatalogName("f"), new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())),
                queryContext,
                "filePath",
                0x40302010,
                false,
                "test-query-id");
        List<MatchNode> es = List.of(new LogicalMatchNode(
                MatchNodeType.MATCH_NODE_TYPE_AND,
                List.of(convertLuceneMatchDataToMatchParams(luceneQueryMatchData0, 0),
                        new LogicalMatchNode(
                                MatchNodeType.MATCH_NODE_TYPE_OR,
                                List.of(convertLuceneMatchDataToMatchParams(luceneQueryMatchData1, 1),
                                        convertLuceneMatchDataToMatchParams(luceneQueryMatchData2, 2)),
                                MemorySegment.ofArray(new byte[10]))),
                MemorySegment.ofArray(new byte[10])));
        assertThat(queryParams.getRootMatchNode().orElseThrow()).isEqualTo(es.getFirst());
        assertThat(queryParams.getNumLucene()).isEqualTo(3);
    }

    private WarmupElementMatchParams convertLuceneMatchDataToMatchParams(LuceneQueryMatchData luceneQueryMatchData, int luceneIx)
    {
        return new WarmupElementMatchParams(
                Arena.ofAuto().allocate(96, 8), // large enough size aligned to size 8 to hold match parameters
                luceneQueryMatchData.getPredicateCacheData().getPredicateBufferInfo().buff(),
                luceneQueryMatchData.getWarmUpElement().getQueryOffset(),
                luceneQueryMatchData.getWarmUpElement().getRecTypeCode(),
                luceneQueryMatchData.getWarmUpElement().getRecTypeLength(),
                luceneQueryMatchData.getWarmUpElement().getWarmUpType(),
                luceneQueryMatchData.getWarmUpElement().getQueryReadSize(),
                MatchCollectOp.MATCH_COLLECT_OP_INVALID,
                -1,
                luceneQueryMatchData.getWarmUpElement().getWarmupElementStats().getNullsCount() > 0 && luceneQueryMatchData.isCollectNulls(),
                luceneQueryMatchData.isTightnessRequired(),
                luceneQueryMatchData.getWarmUpElement().getWarmEvents(),
                luceneQueryMatchData.getWarmUpElement().isImported(),
                Optional.of(new WarmupElementLuceneParams(luceneQueryMatchData, luceneIx)),
                Arena.ofAuto().allocate(16, 8), // large enough size aligned to size 8 to hold node attributes
                0);
    }

    private static LuceneQueryMatchData createLuceneQueryMatchData(String columnName, int weId)
    {
        PredicateCacheData predicateCacheData = mock(PredicateCacheData.class);
        PredicateBufferInfo predicateBufferInfo = mock(PredicateBufferInfo.class);
        when(predicateCacheData.getPredicateBufferInfo()).thenReturn(predicateBufferInfo);
        when(predicateBufferInfo.buff()).thenReturn(MemorySegment.NULL);

        return LuceneQueryMatchData.builder()
                .warmUpElement(WarmUpElement.builder()
                        .warmUpType(WarmUpType.WARM_UP_TYPE_LUCENE)
                        .recTypeCode(RecTypeCode.REC_TYPE_VARCHAR)
                        .recTypeLength(100 + weId)
                        .warpColumn(new RegularColumn(columnName))
                        .warmupElementStats(new WarmupElementStats(1000 + weId, Long.MIN_VALUE, Long.MAX_VALUE))
                        .build())
                .predicateCacheData(predicateCacheData)
                .collectNulls(weId % 2 == 0)
                .tightnessRequired(weId % 3 == 0)
                .build();
    }
}
