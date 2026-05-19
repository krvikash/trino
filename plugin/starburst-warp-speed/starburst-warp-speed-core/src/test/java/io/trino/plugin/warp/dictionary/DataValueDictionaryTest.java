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
package io.trino.plugin.warp.dictionary;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.plugin.warp.dictionary.DataValueDictionary.MAX_DICTIONARY_SIZE;
import static io.trino.plugin.warp.util.NodeUtils.mockNodeManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DataValueDictionaryTest
{
    private DictionaryConfig dictionaryConfig;
    private DictionaryCacheService dictionaryCacheService;
    private DictionaryStats dictionaryStats;
    private NodeManager nodeManager;

    static Stream<Arguments> testParamsNumberType()
    {
        return Stream.of(
                arguments(RecTypeCode.REC_TYPE_DECIMAL_SHORT),
                arguments(RecTypeCode.REC_TYPE_BIGINT),
                arguments(RecTypeCode.REC_TYPE_INTEGER));
    }

    @BeforeAll
    public void beforeAll()
    {
        dictionaryConfig = new DictionaryConfig();
        dictionaryConfig.setEnableDictionary(true);
        nodeManager = mockNodeManager();
    }

    @BeforeEach
    public void beforeEach()
    {
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        dictionaryCacheService = spy(new DictionaryCacheService(
                dictionaryConfig,
                metricsManager,
                mock(AttachDictionaryService.class)));
        dictionaryStats = (DictionaryStats) metricsManager.get(DictionaryStats.createKey());
    }

    @RepeatedTest(10)
    public void testDataValuesDictionaryWithConcurrency()
            throws InterruptedException, ExecutionException
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        Slice[] values = {
                Slices.utf8Slice("Finland"),
                Slices.utf8Slice("Russia"),
                Slices.utf8Slice("Latvia"),
                Slices.utf8Slice("Lithuania"),
                Slices.utf8Slice("Poland"),
        };
        Random r = new Random();

        int numberOfThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_VARCHAR;
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getRecTypeLength()).thenReturn(4);

        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, "column0");

        DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);

        WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
        DictionaryKey writeDictionaryKey = writeDictionary.getDictionaryKey();

        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, warmUpElement, null);
        assertThat(dictionaryState).isEqualTo(DictionaryState.DICTIONARY_VALID);

        DataValueDictionary dataValueDictionary = new DataValueDictionary(dictionaryConfig, writeDictionaryKey, 0, 0, dictionaryStats);
        Map<Slice, Short> validateDictionary = new HashMap<>();

        List<Future<?>> futures = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            futures.add(executor.submit(() -> {
                int offset = 0;
                for (int j = 0; j < 4; j++) {
                    Slice key = values[r.nextInt(values.length)];
                    short index = dataValueDictionary.get(key);

                    putAndAssert(validateDictionary, key, index);

                    offset += dictionaryCacheService.writeDictionary(
                            writeDictionaryKey,
                            recTypeCode,
                            offset,
                            "rowGroupFilePath");
                }
            }));
        }
        // wait for all futures to finish while making sure no ExecutionException has been thrown
        for (Future<?> future : futures) {
            future.get();
        }
        DictionaryToWrite dictionaryToWrite = dataValueDictionary.createDictionaryToWrite();
        List<Slice> originalValues = Arrays.asList(values);
        Set<Slice> distinctValues = new HashSet<>();
        for (short i = 0; i < dictionaryToWrite.getSize(); i++) {
            Slice value = (Slice) dictionaryToWrite.get(i);
            assertThat(originalValues.contains(value)).isTrue();
            distinctValues.add(value);
        }
        assertThat(distinctValues.size()).isEqualTo(dictionaryToWrite.getSize());
        assertThat(dictionaryStats.getwrite_dictionaries_count()).isEqualTo(1);
    }

    @RepeatedTest(10)
    public void testConcurrentWriteDictionaryCreation()
            throws InterruptedException, ExecutionException
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();

        int numberOfThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<String> columns = new ArrayList<>();
        IntStream.range(0, numberOfThreads).forEach(x -> columns.add("column=" + x));

        SchemaTableName schemaTableName = new SchemaTableName("schema", "table");
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_VARCHAR;

        List<Future<?>> futures = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            int threadNumber = i;
            futures.add(executor.submit(() -> {
                long createdTimestamp = DictionaryKey.CREATED_TIMESTAMP_UNKNOWN;
                for (int j = 0; j < 4; j++) {
                    SchemaTableColumn schemaTableColumn = new SchemaTableColumn(schemaTableName, columns.get(threadNumber));
                    DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, createdTimestamp);

                    WarmUpElement localWarmUpElement = mock(WarmUpElement.class);
                    when(localWarmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
                    when(localWarmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
                    when(localWarmUpElement.getRecTypeLength()).thenReturn(4);

                    assertThat(dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, localWarmUpElement, null))
                            .isEqualTo(DictionaryState.DICTIONARY_VALID);

                    WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
                    createdTimestamp = writeDictionary.getDictionaryKey().createdTimestamp();
                }
            }));
        }

        // wait for all futures to finish while making sure no ExecutionException has been thrown
        for (Future<?> future : futures) {
            future.get();
        }

        assertThat(dictionaryStats.getwrite_dictionaries_count()).isEqualTo(numberOfThreads);
    }

    @Test
    public void testFailedMaxDistinctElements()
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("schema", "table"), "column");
        DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        RecTypeCode recTypeCode = RecTypeCode.REC_TYPE_BIGINT;
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getRecTypeLength()).thenReturn(4);
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, warmUpElement, null);
        assertThat(dictionaryState).isEqualTo(DictionaryState.DICTIONARY_VALID);
        DataValueDictionary dataValueDictionary = (DataValueDictionary) dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
        int overMaxLimit = dictionaryConfig.getDictionaryMaxSize() + 1;
        DictionaryException exception = Assertions.assertThrows(DictionaryException.class, () -> {
            for (long j = 0; j < overMaxLimit; j++) {
                dataValueDictionary.get(j);
            }
        });
        assertThat(exception.getDictionaryState()).isEqualTo(DictionaryState.DICTIONARY_MAX_EXCEPTION);
        assertThat(dataValueDictionary.getDictionaryWeight()).isLessThan(MAX_DICTIONARY_SIZE);
        assertThat(dataValueDictionary.getWriteSize()).isEqualTo(dictionaryConfig.getDictionaryMaxSize());
    }

    /**
     * in case of a number type column we shouldn't over MAX_DICTIONARY_SIZE
     */
    @ParameterizedTest
    @MethodSource("testParamsNumberType")
    public void testDictionaryNotOverSizeLimit(RecTypeCode recTypeCode)
    {
        String nodeIdentifier = nodeManager.getCurrentNode().getNodeIdentifier();
        SchemaTableColumn schemaTableColumn = new SchemaTableColumn(new SchemaTableName("schema", "table"), "column");
        DictionaryKey dictionaryKey = new DictionaryKey(schemaTableColumn, nodeIdentifier, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        when(warmUpElement.getRecTypeCode()).thenReturn(recTypeCode);
        when(warmUpElement.getRecTypeLength()).thenReturn(4);
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, warmUpElement, null);
        assertThat(dictionaryState).isEqualTo(DictionaryState.DICTIONARY_VALID);
        DataValueDictionary dataValueDictionary = (DataValueDictionary) dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode);
        for (long j = 0; j < dictionaryConfig.getDictionaryMaxSize(); j++) {
            dataValueDictionary.get(j);
        }
        assertThat(dataValueDictionary.getDictionaryWeight()).isLessThan(MAX_DICTIONARY_SIZE);
        assertThat(dataValueDictionary.getWriteSize()).isEqualTo(dictionaryConfig.getDictionaryMaxSize());
    }

    private void putAndAssert(Map<Slice, Short> resultMap, Slice s1, short a)
    {
        Short put = resultMap.put(s1, a);
        if (put != null) {
            assertThat(put).isEqualTo(a);
        }
    }
}
