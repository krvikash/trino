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
package io.trino.operator.torin;

public final class BenchmarkGpuUtils
{
    private BenchmarkGpuUtils() {}

    /// Sizes the GPU memory pool like the default configuration does, to nearly all device memory, so a
    /// benchmark can evaluate batches as large as production ones. A benchmark's own `@Test` runs among
    /// other tests and uses [GpuTestUtils#maybeSetGpuMemoryPoolForTests()] instead.
    public static void setGpuMemoryPoolForBenchmarks()
    {
        new GpuConfigurer(new GpuConfig()).setup();
    }
}
