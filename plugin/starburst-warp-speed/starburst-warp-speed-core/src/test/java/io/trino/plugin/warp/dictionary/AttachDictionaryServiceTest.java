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

import io.airlift.slice.Slices;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.dictionary.DictionaryWriterFactory;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static io.trino.plugin.warp.dictionary.DictionariesCacheTest.buildDictionaryKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AttachDictionaryServiceTest
{
    private AttachDictionaryService attachDictionaryService;
    private final GlobalConfig globalConfig = new GlobalConfig();

    @BeforeEach
    public void before()
    {
        globalConfig.setLocalStorePath("/tmp/test/");

        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);
        when(storageEngineConstants.getPageOffsetMask()).thenReturn(8192 - 1);

        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        DictionaryWriterFactory dictionaryWriterFactory = new DictionaryWriterFactory();

        attachDictionaryService = new AttachDictionaryService(mock(WorkerCapacityManager.class),
                storageEngineConstants,
                new DictionaryConfig(),
                metricsManager,
                dictionaryWriterFactory);
    }

    @Test
    public void testReadWriteDictionaryInt()
            throws IOException
    {
        Integer[] intDictionary = {1, 2};
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(intDictionary, 2, 4, 8);
        DictionaryKey dictionaryKey = buildDictionaryKey("int1");
        String rowGroupFilePath = globalConfig.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_INTEGER, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_INTEGER, 4, 0, rowGroupFilePath);

        assertThat(res.isImmutable()).isTrue();
        assertThat(res.getWriteSize()).isEqualTo(2);
        assertThat(res.getDictionaryWeight()).isEqualTo(8);
        assertThat(res.get(0)).isEqualTo(1);
        assertThat(res.get(1)).isEqualTo(2);

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    public void testReadWriteDictionaryFixedLengthChar()
            throws IOException
    {
        io.airlift.slice.Slice[] values = {Slices.utf8Slice("aaa"),
                Slices.utf8Slice("bbbb"),
                Slices.utf8Slice("cccc"),
                Slices.utf8Slice("dddd"),
                Slices.utf8Slice("eeee"),
        };
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(values, 5, 4, 19);
        DictionaryKey dictionaryKey = buildDictionaryKey("char4");
        String rowGroupFilePath = globalConfig.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_CHAR, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_CHAR, 4, 0, rowGroupFilePath);

        assertThat(res.isImmutable()).isTrue();
        assertThat(res.getWriteSize()).isEqualTo(5);
        assertThat(res.getDictionaryWeight()).isEqualTo(20);
        assertThat(((io.airlift.slice.Slice) res.get(0)).toStringUtf8()).isEqualTo("aaa");
        assertThat(((io.airlift.slice.Slice) res.get(1)).toStringUtf8()).isEqualTo("bbbb");
        assertThat(((io.airlift.slice.Slice) res.get(2)).toStringUtf8()).isEqualTo("cccc");
        assertThat(((io.airlift.slice.Slice) res.get(3)).toStringUtf8()).isEqualTo("dddd");
        assertThat(((io.airlift.slice.Slice) res.get(4)).toStringUtf8()).isEqualTo("eeee");

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    public void testReadWriteDictionaryChangedLengthChar()
            throws IOException
    {
        io.airlift.slice.Slice[] values = {Slices.utf8Slice("1"),
                Slices.utf8Slice("88888888"),
                Slices.utf8Slice("333"),
                Slices.utf8Slice("4444"),
                Slices.utf8Slice("00"),
        };
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(values, 5, 256, 18);
        DictionaryKey dictionaryKey = buildDictionaryKey("varchar");
        String rowGroupFilePath = globalConfig.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation();
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_VARCHAR, 0, rowGroupFilePath);
        DataValueDictionary res = attachDictionaryService.load(dictionaryKey, RecTypeCode.REC_TYPE_VARCHAR, 256, 0, rowGroupFilePath);

        assertThat(res.isImmutable()).isTrue();
        assertThat(res.getWriteSize()).isEqualTo(5);
        assertThat(res.getDictionaryWeight()).isEqualTo(18);
        assertThat(((io.airlift.slice.Slice) res.get(0)).toStringUtf8()).isEqualTo("1");
        assertThat(((io.airlift.slice.Slice) res.get(1)).toStringUtf8()).isEqualTo("88888888");
        assertThat(((io.airlift.slice.Slice) res.get(2)).toStringUtf8()).isEqualTo("333");
        assertThat(((io.airlift.slice.Slice) res.get(3)).toStringUtf8()).isEqualTo("4444");
        assertThat(((io.airlift.slice.Slice) res.get(4)).toStringUtf8()).isEqualTo("00");

        FileUtils.deleteQuietly(Path.of(rowGroupFilePath).toFile());
    }

    @Test
    void test_save_IOException()
    {
        Integer[] intDictionary = {1, 2};
        DictionaryToWrite dictionaryToWrite = new DictionaryToWrite(intDictionary, 2, 4, 8);
        int dictionaryOffset = Integer.MAX_VALUE;
        DictionaryKey dictionaryKey = buildDictionaryKey("int1");
        String rowGroupFilePath = globalConfig.getLocalStorePath() + dictionaryKey.stringFileNameRepresentation() + "/// ...";

        assertThatThrownBy(() ->
                attachDictionaryService.save(dictionaryToWrite, RecTypeCode.REC_TYPE_INTEGER, dictionaryOffset, rowGroupFilePath))
                .isInstanceOf(RuntimeException.class);
    }
}
