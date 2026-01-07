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

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.gen.stats.LuceneIndexerStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LuceneIndexerTest
{
    private static final Logger log = Logger.get(LuceneIndexerTest.class);

    private static Path localStorePath;

    private final StubsStorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
    private final LuceneIndexerStats stats = new LuceneIndexerStats();

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("LuceneIndexerTest");
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

    @Test
    void testImmenseValue()
    {
        StringBuilder huge = new StringBuilder();

        for (int i = 0; i < 2048; i++) {
            String value = "testHugeStringValue" + i;
            huge.append(value);
        }

        String rowGroupFilePath = localStorePath + "/tmp/rowGroupFilePath/testImmenseValue";
        LuceneIndexer luceneIndexer = new LuceneIndexer(
                storageEngineConstants,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                rowGroupFilePath,
                stats);
        luceneIndexer.resetLuceneIndex();

        Assertions.assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> luceneIndexer.addDoc(Slices.utf8Slice("n1"), Slices.utf8Slice(huge.toString()), Slices.utf8Slice("n2")));

        String writeLockFileName = rowGroupFilePath + "/write.lock";
        File writeLockFile = new File(writeLockFileName);

        Assertions.assertThat(writeLockFile.exists()).isFalse();

        LuceneIndexer luceneIndexer1 = new LuceneIndexer(
                storageEngineConstants,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()),
                rowGroupFilePath,
                stats);

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
