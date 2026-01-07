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
package io.trino.plugin.warp.dal.dispatcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.DictionaryInfo;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.ExportState;
import io.trino.plugin.warp.dispatcher.model.FastWarmingState;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.model.WildcardColumn;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.plugin.warp.util.json.SliceSerializer;
import io.trino.plugin.warp.util.json.WarpColumnJsonKeyDeserializer;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowGroupDataDaoTest
{
    private static final Logger log = Logger.get(RowGroupDataDaoTest.class);

    private static Path localStorePath;

    private GlobalConfig globalConfig;
    private StorageEngineConstants storageEngineConstants;
    private RowGroupDataDao rowGroupDataDao;

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("RowGroupDataDaoTest");
    }

    @AfterAll
    static void afterAll()
    {
        try {
            deleteRecursively(localStorePath, ALLOW_INSECURE);
        }
        catch (IOException e) {
            // TODO this probably should be propagated
            log.error(e, "Failed to delete localStorePath '%s'", localStorePath);
        }
        localStorePath = null;
    }

    @BeforeEach
    public void before()
    {
        globalConfig = new GlobalConfig();
        globalConfig.setLocalStorePath(localStorePath.toFile().getAbsolutePath());

        storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);

        rowGroupDataDao = new RowGroupDataDao(
                globalConfig,
                storageEngineConstants,
                new ObjectMapperProvider(),
                new ShapingLoggerFactory(new CatalogName("catalog"), new SharedConfig()));
    }

    private void createFileIfNeeded(String filePath)
    {
        File rowGroupDataFile = new File(filePath);

        if (!rowGroupDataFile.exists()) {
            try {
                FileUtils.createParentDirectories(rowGroupDataFile);
                boolean newFile = rowGroupDataFile.createNewFile();
                if (!newFile) {
                    throw new RuntimeException("failed creating file " + rowGroupDataFile.getAbsolutePath());
                }
                Writer writer = Files.newBufferedWriter(rowGroupDataFile.toPath(), UTF_8);
                writer.write(StringUtils.randomAlphanumeric(8192));
                writer.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testSimpleCRUD()
    {
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warpColumn(new RegularColumn("aaa"))
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        List<WarmUpElement> warmUpElements = List.of(warmUpElement);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        "file_path",
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .build();

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isNull();

        rowGroupDataDao.save(List.of(rowGroupData));

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        assertThat(rowGroupDataDao.getAll()).hasSize(1).containsExactlyElementsOf(List.of(rowGroupData));

        //test merge
        Collection<RowGroupData> updateRowGroupDataList = rowGroupDataDao.save(List.of(RowGroupData.builder(rowGroupData)
                .partitionKeys(Map.of(new RegularColumn("key"), "val"))
                .build()));

        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement())).isNotEqualTo(rowGroupData);
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement()).getLock()).isEqualTo(rowGroupData.getLock());
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement())).isEqualTo(rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
        assertThat(updateRowGroupDataList.stream().collect(MoreCollectors.onlyElement()).getLock()).isEqualTo(rowGroupDataDao.get(rowGroupData.getRowGroupKey()).getLock());

        rowGroupDataDao.delete(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.getAll()).isEmpty();
    }

    @Test
    public void testLongFilePath()
    {
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warpColumn(new RegularColumn("aaa"))
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .build();

        List<WarmUpElement> warmUpElements = List.of(warmUpElement);

        String validFilePath = StringUtils.randomAlphanumeric(246);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        validFilePath,
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .nextOffset(1)
                .build();

        createFileIfNeeded(rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath()));

        rowGroupDataDao.save(List.of(rowGroupData));
        rowGroupDataDao.flush(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        String nonValidFilePath = StringUtils.randomAlphanumeric(260);

        RowGroupKey nonValidRowGroupKey = new RowGroupKey(
                "schema",
                "table",
                nonValidFilePath,
                0,
                1L,
                0,
                "",
                "");

        assertThatThrownBy(() -> createFileIfNeeded(nonValidRowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath())))
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("Cannot create directory");
    }

    @Test
    public void testSerialization()
    {
        WarpColumn warpColumn = new RegularColumn("aaa");
        String nodeIdentifier = "node1";
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(warpColumn)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                .usedDictionarySize(7)
                .dictionaryInfo(new DictionaryInfo(
                        new DictionaryKey(
                                new SchemaTableColumn(
                                        new SchemaTableName("schema", "table"), warpColumn),
                                nodeIdentifier,
                                54321),
                        DictionaryState.DICTIONARY_IMPORTED,
                        3,
                        6))
                .startOffset(1)
                .queryOffset(2)
                .queryReadSize(3)
                .totalRecords(7)
                .warmEvents(4)
                .endOffset(5)
                .state(WarmUpElementState.FAILED_PERMANENTLY)
//                .lastUsedTimestamp()      <== transient
                .exportState(ExportState.FAILED_PERMANENTLY)
                .isImported(true)
                .warmState(WarmState.WARM)
                .build();
        List<WarmUpElement> warmUpElements = List.of(warmUpElement);
        String validFilePath = StringUtils.randomAlphanumeric(246);
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        validFilePath,
                        0,
                        1L,
                        123456789,
                        "",
                        ""))
                .warmUpElements(warmUpElements)
                .partitionKeys(Map.of(new RegularColumn("key1"), "val1"))
                .nodeIdentifier(nodeIdentifier)
                .nextOffset(2)
                .nextExportOffset(3)
                .sparseFile(true)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .dataValidation(new RowGroupDataValidation("etag", 987654321, 918273645))
                .build();

        createFileIfNeeded(rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath()));

        rowGroupDataDao.save(List.of(rowGroupData));
        rowGroupDataDao.flush(rowGroupData.getRowGroupKey());

        assertThat(rowGroupDataDao.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);

        //read from another dao
        RowGroupDataDao rowGroupDataDao2 = new RowGroupDataDao(
                globalConfig,
                storageEngineConstants,
                new ObjectMapperProvider(),
                new ShapingLoggerFactory(new CatalogName("catalog"), new SharedConfig()));
        assertThat(rowGroupDataDao2.get(rowGroupData.getRowGroupKey())).isEqualTo(rowGroupData);
    }

    @Test
    public void testSerializeAndDeserialize()
            throws JsonProcessingException
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(new TestingTypeManager())));
        provider.withKeyDeserializers(ImmutableMap.of(
                WarpColumn.class, new WarpColumnJsonKeyDeserializer()));
        ObjectMapper objectMapper = provider.get();

        //DictionaryKey
        DictionaryKey dictionaryKey = new DictionaryKey(
                new SchemaTableColumn(
                        new SchemaTableName("schema1", "table1"),
                        "column1"),
                "nodeIdentifier111",
                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        String str = objectMapper.writerFor(dictionaryKey.getClass()).writeValueAsString(dictionaryKey);
        assertThat(dictionaryKey)
                .isEqualTo(objectMapper.readValue(str, dictionaryKey.getClass()));

        //WarpColumn
        List<WarpColumn> warpColumns = List.of(
                new RegularColumn("aaa"),
                new WildcardColumn(),
                new TransformedColumn("aaa", "bbbb", new TransformFunction(TransformFunction.TransformType.DATE, List.of(new WarpPrimitiveConstant(1, IntegerType.INTEGER)))));
        str = objectMapper.writerFor(new TypeReference<List<WarpColumn>>() {}).writeValueAsString(warpColumns);
        assertThat(warpColumns).isEqualTo(objectMapper.readValue(str, new TypeReference<List<WarpColumn>>() {}));

        // WarmUpElement
        WarmUpElement warmUpElement = WarmUpElement.builder()
                .warpColumn(new RegularColumn("aaa"))
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                .recTypeLength(4)
                .warmupElementStats(new WarmupElementStats(0, Slices.wrappedBuffer(new byte[] {1}), Slices.wrappedBuffer(new byte[] {9})))
                .usedDictionarySize(7)
                .dictionaryInfo(new DictionaryInfo(
                        new DictionaryKey(
                                new SchemaTableColumn(
                                        new SchemaTableName("schema1", "table1"),
                                        "column1"),
                                "nodeIdentifier111",
                                DictionaryKey.CREATED_TIMESTAMP_UNKNOWN),
                        DictionaryState.DICTIONARY_IMPORTED,
                        4,
                        DictionaryInfo.NO_OFFSET))
                .startOffset(1)
                .queryOffset(2)
                .totalRecords(7)
                .queryReadSize(3)
                .warmEvents(4)
                .endOffset(5)
                .state(WarmUpElementState.VALID)
//                .lastUsedTimestamp()      <== transient
                .exportState(ExportState.EXPORTED)
                .isImported(true)
                .warmState(WarmState.WARM)
                .build();

        str = objectMapper.writeValueAsString(warmUpElement);
        assertThat(objectMapper.readValue(str, WarmUpElement.class)).isEqualTo(warmUpElement);

        //RowGroupData
        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(new RowGroupKey(
                        "schema",
                        "table",
                        StringUtils.randomAlphanumeric(10),
                        0,
                        1L,
                        0,
                        "",
                        ""))
                .warmUpElements(List.of(warmUpElement))
                .partitionKeys(Map.of(new RegularColumn("key1"), "val1"))
                .nodeIdentifier("node1")
                .nextOffset(2)
                .nextExportOffset(3)
                .sparseFile(true)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .dataValidation(new RowGroupDataValidation("etag", 987654321, 918273645))
                .build();

        str = objectMapper.writeValueAsString(rowGroupData);
        assertThat(objectMapper.readValue(str, RowGroupData.class)).isEqualTo(rowGroupData);
    }

    @Disabled
    @Test
    public void testMultiThread()
            throws InterruptedException
    {
        int numberOfThreads = 10;
        try (ExecutorService service = Executors.newFixedThreadPool(numberOfThreads)) {
            CountDownLatch latch = new CountDownLatch(numberOfThreads);
            List<WarmUpElement> warmUpElements = List.of(WarmUpElement.builder()
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .recTypeCode(RecTypeCode.REC_TYPE_INTEGER)
                    .recTypeLength(4)
                    .warmupElementStats(new WarmupElementStats(0, Long.MIN_VALUE, Long.MAX_VALUE))
                    .warpColumn(new RegularColumn("aaa"))
                    .build());

            IntStream.range(0, 10).forEach(i -> service.execute(() -> {
                RowGroupData rowGroupData = RowGroupData.builder()
                        .rowGroupKey(new RowGroupKey(
                                "schema",
                                "table",
                                StringUtils.randomAlphanumeric(i),
                                0,
                                1L,
                                0,
                                "",
                                ""))
                        .warmUpElements(warmUpElements)
                        .build();
                int waitTime = Math.abs((new Random(i).nextInt(Integer.MAX_VALUE) % 10) + 1) * 100;
                IntStream.range(0, 3).forEach(j -> {
                    wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                    wait(waitTime, () -> rowGroupDataDao.getAll());
                    wait(waitTime, () -> rowGroupDataDao.save(rowGroupData));
                    wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                    wait(waitTime, () -> rowGroupDataDao.getAll());
                    wait(waitTime, () -> rowGroupDataDao.delete(rowGroupData));
                    wait(waitTime, () -> rowGroupDataDao.get(rowGroupData.getRowGroupKey()));
                    wait(waitTime, () -> rowGroupDataDao.getAll());
                });
                latch.countDown();
            }));
            latch.await();
        }
    }

    private void wait(int i, Runnable runnable)
    {
        try {
            Thread.sleep(i);
        }
        catch (InterruptedException e) {
            //do nothing
        }
        runnable.run();
    }
}
