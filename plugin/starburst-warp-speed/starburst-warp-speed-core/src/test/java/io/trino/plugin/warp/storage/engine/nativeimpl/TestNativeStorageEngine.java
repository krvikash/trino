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
package io.trino.plugin.warp.storage.engine.nativeimpl;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageEngine.calculateStorageCacheSizeInPages;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNativeStorageEngine
{
    private static final long GIGABYTE = 1024L * 1024 * 1024;
    private static final int BYTES_PER_PAGE = 9100;

    @Test
    public void testStorageCacheSizeFillsBudgetWithoutExceedingIt()
    {
        long budget = (128 * GIGABYTE - 10 * GIGABYTE) * 25 / 100;
        int pages = calculateStorageCacheSizeInPages(25, 128 * GIGABYTE, 10 * GIGABYTE, BYTES_PER_PAGE);
        assertThat((long) pages * BYTES_PER_PAGE).isLessThanOrEqualTo(budget);
        assertThat((long) (pages + 1) * BYTES_PER_PAGE).isGreaterThan(budget);
    }

    @Test
    public void testStorageCacheSizeIsClampedToNativeLimits()
    {
        assertThat(calculateStorageCacheSizeInPages(1, 4 * GIGABYTE, 3 * GIGABYTE, BYTES_PER_PAGE)).isEqualTo(32 * 1024);
        assertThat(calculateStorageCacheSizeInPages(90, 2048 * GIGABYTE, 10 * GIGABYTE, BYTES_PER_PAGE)).isEqualTo(16 * 1024 * 1024);
    }
}
