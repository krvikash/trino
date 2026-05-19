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
package io.trino.plugin.warp.storage.capacity;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.tools.util.PathUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class WorkerCapacityManagerTest
{
    private WorkerCapacityManager workerCapacityManager;
    private GlobalConfig globalConfig;
    private CatalogNameProvider catalogNameProvider;
    private final Random random = new Random();

    @BeforeEach
    public void before()
    {
        globalConfig = new GlobalConfig();
        catalogNameProvider = new CatalogNameProvider("CatalogName");
        workerCapacityManager = new WorkerCapacityManager(
                globalConfig,
                new WarmupDemoterConfig(),
                new StubsStorageEngineConstants(),
                mock(NativeStorageStateHandler.class),
                TestingTxService.createMetricsManager(),
                catalogNameProvider);
    }

    @Test
    void testInitWorker()
    {
        Path localStorePath;
        try {
            localStorePath = Files.createTempDirectory("cleanLocalStorage1");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        File localStoreDirectory = localStorePath.toFile();
        globalConfig.setLocalStorePath(localStoreDirectory.getAbsolutePath());

        String catalogLocalStorePath = PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get());
        File catalogLocalStore = new File(catalogLocalStorePath);
        catalogLocalStore.mkdirs();

        try {
            int numFiles = 10;
            for (int i = 0; i < numFiles; i++) {
                String tempFileName = String.format(Locale.US, catalogLocalStorePath + "/test/case-%d/bucket/schema/table/part-%05d-7a144fa0-52b0-473d-b1ab-a5dbbadd01ae-c000.snappy.parquet/0/110606/", i, i);
                new File(tempFileName).mkdirs();
                File tempFile = new File(tempFileName + "1549797223000-" + i);

                if (tempFile.createNewFile()) {
                    byte[] array = new byte[8192];
                    random.nextBytes(array);
                    String generatedString = new String(array, UTF_8);

                    Writer writer = Files.newBufferedWriter(tempFile.toPath(), UTF_8);
                    writer.write(generatedString);
                    writer.close();
                }
                else {
                    throw new RuntimeException("failed to create file");
                }
            }
            Failsafe.with(RetryPolicy.builder()
                            .handle(AssertionError.class)
                            .withDelay(Duration.ofMillis(10))
                            .withMaxRetries(10)
                            .build())
                    .run(() -> Assertions.assertEquals(1, requireNonNull(catalogLocalStore.list()).length));

            workerCapacityManager.initWorker();

            Failsafe.with(RetryPolicy.builder()
                            .handle(AssertionError.class)
                            .withDelay(Duration.ofMillis(10))
                            .withMaxRetries(10)
                            .build())
                    .run(() -> {
                        assertThat(workerCapacityManager.isWorkerInitialized()).isTrue();
                        assertThat(workerCapacityManager.getTotalCapacity()).isGreaterThan(0);
                    });

            Assertions.assertEquals(0, requireNonNull(catalogLocalStore.list()).length);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            FileUtils.deleteQuietly(localStoreDirectory);
        }
    }

    @Test
    void testCatalogLocalStore()
    {
        RowGroupKey rowGroupKey = new RowGroupKey(
                "s",
                "t",
                "fl",
                1L,
                2L,
                3L,
                null,
                catalogNameProvider.get());

        assertThat(rowGroupKey.stringFileNameRepresentation("localStorePath"))
                .startsWith("localStorePath/" + catalogNameProvider.get());
    }
}
