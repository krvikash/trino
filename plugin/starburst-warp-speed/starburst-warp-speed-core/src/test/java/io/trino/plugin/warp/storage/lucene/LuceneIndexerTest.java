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
package io.trino.plugin.warp.storage.lucene;

import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.gen.stats.LuceneIndexerStats;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.TrinoException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static io.trino.plugin.warp.storage.write.StorageWriterService.LUCENE_STATS_GROUP_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LuceneIndexerTest
{
    private static Path localStorePath;

    private final StubsStorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
    private final LuceneIndexerStats stats = new LuceneIndexerStats(LUCENE_STATS_GROUP_NAME, "0");
    private final GlobalConfig globalConfig = new GlobalConfig();

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("LuceneIndexerTest");
    }

    @AfterAll
    static void afterAll()
    {
        if (Objects.nonNull(localStorePath)) {
            try (Stream<Path> stream = Files.walk(localStorePath)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            catch (IOException e) {
                System.out.printf("failed to delete localStorePath '%s'%n", localStorePath);
            }
        }
    }

    @Test
    void testImmenseValue()
    {
        StringBuilder huge = new StringBuilder();

        for (int i = 0; i < 2048; i++) {
            String value = "testHugeStringValue" + i;
            huge.append(value);
        }

        String rowGroupFilePath = localStorePath + "/tmp/rowGroupFilePath/testImmenseValue";
        LuceneIndexer luceneIndexer = new LuceneIndexer(storageEngineConstants, rowGroupFilePath, stats, globalConfig);
        luceneIndexer.resetLuceneIndex();

        Assertions.assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> luceneIndexer.addDoc(Slices.utf8Slice("n1"), Slices.utf8Slice(huge.toString()), Slices.utf8Slice("n2")));

        String writeLockFileName = rowGroupFilePath + "/write.lock";
        File writeLockFile = new File(writeLockFileName);

        Assertions.assertThat(writeLockFile.exists()).isFalse();

        LuceneIndexer luceneIndexer1 = new LuceneIndexer(storageEngineConstants, rowGroupFilePath, stats, globalConfig);

        Assertions.assertThatNoException().isThrownBy(luceneIndexer1::resetLuceneIndex);
    }

    @Test
    void testMaxTermLength()
    {
        String string = "Hello, Ω!"; // Unicode string
        Assertions.assertThat(string.length()).isEqualTo(9);
        Assertions.assertThat(string.getBytes(UTF_8).length).isEqualTo(10);
    }
}
