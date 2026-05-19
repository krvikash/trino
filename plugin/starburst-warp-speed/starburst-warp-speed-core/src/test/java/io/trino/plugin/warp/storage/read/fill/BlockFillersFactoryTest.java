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
package io.trino.plugin.warp.storage.read.fill;

import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dictionary.AttachDictionaryService;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class BlockFillersFactoryTest
{
    @Test
    public void test_real()
    {
        DictionaryConfig dictionaryConfig = new DictionaryConfig();
        dictionaryConfig.setEnableDictionary(true);
        DictionaryCacheService dictionaryCacheService = new DictionaryCacheService(
                dictionaryConfig,
                TestingTxService.createMetricsManager(),
                mock(AttachDictionaryService.class));
        BlockFillersFactory blockFillersFactory = new BlockFillersFactory(
                dictionaryCacheService,
                new StubsStorageEngineConstants(),
                new NativeConfig());
        BlockFiller blockFiller = blockFillersFactory.getBlockFiller(RecTypeCode.REC_TYPE_REAL.ordinal());
        assertThat(blockFiller).isInstanceOf(IntBlockFiller.class);
    }
}
