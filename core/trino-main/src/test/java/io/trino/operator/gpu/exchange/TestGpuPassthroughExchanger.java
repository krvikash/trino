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
package io.trino.operator.gpu.exchange;

import io.trino.operator.exchange.LocalExchangeMemoryManager;
import io.trino.operator.gpu.GpuTestUtils;
import io.trino.spi.gpu.GpuPage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuPassthroughExchanger
{
    @Test
    public void forwardsToSingleBuffer()
    {
        GpuTestUtils.maybeSetGpuMemoryPoolForTests();
        LocalExchangeMemoryManager memory = new LocalExchangeMemoryManager(1 << 20);
        GpuLocalExchangeBuffer buffer = new GpuLocalExchangeBuffer(memory, _ -> {});
        GpuExchanger exchanger = new GpuPassthroughExchanger(buffer, memory);

        try (GpuPage input = GpuTestUtils.deviceIntColumn(new int[] {1, 2, 3})) {
            exchanger.accept(input);
        }

        try (GpuPage page = buffer.removePage()) {
            assertThat(page.positionCount()).isEqualTo(3);
        }
    }
}
