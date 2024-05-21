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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommonStoreIdFinderTest
{
    static Stream<Arguments> storeIdTest()
    {
        return Stream.of(
                arguments(List.of(List.of("c1")),
                        List.of("c1"), true),
                arguments(List.of(List.of("c1"), List.of("c1", "c2")),
                        List.of("c1"), true),
                arguments(List.of(List.of("c1"), List.of("c1", "c2")),
                        List.of("c1", "c3"), false),
                arguments(List.of(List.of("c1"), List.of("c4")),
                        List.of("c1", "c4", "c5"), false),
                arguments(List.of(List.of("c1"), List.of("c4"), List.of("c1", "c4", "c5")),
                        List.of("c1", "c4", "c5"), true),
                arguments(List.of(List.of("c1")),
                        List.of("c2"), false));
    }

    @ParameterizedTest
    @MethodSource("storeIdTest")
    public void testGetQueryStoreId(List<List<String>> warmedColumns, List<String> requiredColumns, boolean hasStoreId)
    {
        RowGroupData cacheRowGroupData = createCacheRowGroupData(warmedColumns);
        RowGroupDataService rowGroupDataService = mock(RowGroupDataService.class);
        RowGroupKey rowGroupKey = cacheRowGroupData.getRowGroupKey();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(cacheRowGroupData);

        List<CacheColumnId> planSignatureColumns = requiredColumns.stream().map(CacheColumnId::new).toList();
        PlanSignature planSignature = mock(PlanSignature.class);
        when(planSignature.getColumns()).thenReturn(planSignatureColumns);

        CommonStoreIdFinder commonStoreIdFinder = new CommonStoreIdFinder(rowGroupDataService, planSignature);
        Optional<UUID> queryStoreId = commonStoreIdFinder.findAndCache(rowGroupKey);
        assertThat(queryStoreId.isPresent()).isEqualTo(hasStoreId);
    }

    private RowGroupData createCacheRowGroupData(List<List<String>> warmedColumns)
    {
        RowGroupData cachedRowGroupData = mock(RowGroupData.class);
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        for (var warmedSession : warmedColumns) {
            List<WarmUpElement> weWithSameStoreId = createWeWithSameStoreId(warmedSession);
            warmUpElementList.addAll(weWithSameStoreId);
        }
        when(cachedRowGroupData.getValidWarmUpElements()).thenReturn(warmUpElementList);
        when(cachedRowGroupData.getRowGroupKey()).thenReturn(mock(RowGroupKey.class));
        return cachedRowGroupData;
    }

    private List<WarmUpElement> createWeWithSameStoreId(List<String> columns)
    {
        UUID storeId = UUID.randomUUID();
        List<WarmUpElement> warmUpElementList = new ArrayList<>();
        for (String column : columns) {
            WarmUpElement we = mock(WarmUpElement.class);
            when(we.getStoreId()).thenReturn(storeId);
            when(we.getVaradaColumn()).thenReturn(new RegularColumn(column));
            warmUpElementList.add(we);
        }
        return warmUpElementList;
    }
}
