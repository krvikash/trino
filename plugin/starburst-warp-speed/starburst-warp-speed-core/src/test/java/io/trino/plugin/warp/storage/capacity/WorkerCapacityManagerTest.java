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
import java.util.concurrent.CountDownLatch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
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
                    .run(() -> assertThat(requireNonNull(catalogLocalStore.list()).length).isEqualTo(1));

            workerCapacityManager.initWorker();

            // cleanup and the capacity calculation finish on a background thread, which can take seconds on slow CI
            Failsafe.with(RetryPolicy.builder()
                            .handle(AssertionError.class)
                            .withDelay(Duration.ofMillis(100))
                            .withMaxRetries(50)
                            .build())
                    .run(() -> {
                        assertThat(workerCapacityManager.isWorkerInitialized()).isTrue();
                        assertThat(workerCapacityManager.getTotalCapacity()).isGreaterThan(0);
                    });

            assertThat(requireNonNull(catalogLocalStore.list()).length).isEqualTo(0);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            FileUtils.deleteQuietly(localStoreDirectory);
        }
    }

    @Test
    void testNotInitializedWhileCleanupRuns()
            throws Exception
    {
        Path localStorePath = Files.createTempDirectory("cleanLocalStorage2");
        globalConfig.setLocalStorePath(localStorePath.toString());
        File catalogLocalStore = new File(PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get()));
        assertThat(catalogLocalStore.mkdirs()).isTrue();
        Files.writeString(catalogLocalStore.toPath().resolve("leftover"), "data");

        CountDownLatch cleanupRunning = new CountDownLatch(1);
        CountDownLatch releaseCleanup = new CountDownLatch(1);
        NativeStorageStateHandler nativeStorageStateHandler = mock(NativeStorageStateHandler.class);
        doAnswer(_ -> {
            cleanupRunning.countDown();
            releaseCleanup.await();
            return null;
        }).when(nativeStorageStateHandler).enableTemporarily();
        workerCapacityManager = new WorkerCapacityManager(
                globalConfig,
                new WarmupDemoterConfig(),
                new StubsStorageEngineConstants(),
                nativeStorageStateHandler,
                TestingTxService.createMetricsManager(),
                catalogNameProvider);

        try {
            workerCapacityManager.initWorker();
            assertThat(cleanupRunning.await(10, SECONDS)).isTrue();
            assertThat(workerCapacityManager.isWorkerInitialized()).isFalse();
        }
        finally {
            releaseCleanup.countDown();
            FileUtils.deleteQuietly(localStorePath.toFile());
        }
    }

    @Test
    void testInitWorkerOnEmptyStore()
            throws IOException
    {
        Path localStorePath = Files.createTempDirectory("cleanLocalStorage3");
        globalConfig.setLocalStorePath(localStorePath.toString());
        File catalogLocalStore = new File(PathUtils.getUriPath(globalConfig.getLocalStorePath(), catalogNameProvider.get()));
        assertThat(catalogLocalStore.mkdirs()).isTrue();
        try {
            workerCapacityManager.initWorker();
            assertThat(workerCapacityManager.isWorkerInitialized()).isTrue();
            assertThat(workerCapacityManager.getTotalCapacity()).isGreaterThan(0);
        }
        finally {
            FileUtils.deleteQuietly(localStorePath.toFile());
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
