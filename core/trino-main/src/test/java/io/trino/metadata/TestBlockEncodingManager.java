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
package io.trino.metadata;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.FeaturesConfig;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestBlockEncodingManager
{
    @Test
    public void testLookupPerTypeOverrides()
    {
        FeaturesConfig config = new FeaturesConfig();
        config.setExchangeVbyteBlockEncodingEnabled(false);
        BlockEncodingManager manager = new BlockEncodingManager(config);

        // no overrides so far
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.empty()).getClass()).isEqualTo(LongArrayBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(BigintType.BIGINT)).getClass()).isEqualTo(LongArrayBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(DecimalType.createDecimalType(5))).getClass()).isEqualTo(LongArrayBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(DecimalType.createDecimalType(9))).getClass()).isEqualTo(LongArrayBlockEncoding.class);

        // add some overrides
        XBlockEncoding xBlockEncoding = new XBlockEncoding();
        YBlockEncoding yBlockEncoding = new YBlockEncoding();
        manager.addTypeSpecificBlockEncodingOverride(xBlockEncoding, type -> type == BigintType.BIGINT);
        manager.addTypeSpecificBlockEncodingOverride(yBlockEncoding, type -> type instanceof DecimalType decimalType && decimalType.isShort() && decimalType.getPrecision() <= 5);

        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.empty()).getClass()).isEqualTo(LongArrayBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(BigintType.BIGINT)).getClass()).isEqualTo(XBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(DecimalType.createDecimalType(5))).getClass()).isEqualTo(YBlockEncoding.class);
        assertThat(manager.getBlockEncodingByBlockClassAndType(LongArrayBlock.class, Optional.of(DecimalType.createDecimalType(9))).getClass()).isEqualTo(LongArrayBlockEncoding.class);
    }

    private static class XBlockEncoding
            implements BlockEncoding
    {
        @Override
        public String getName()
        {
            return "X";
        }

        @Override
        public Class<? extends Block> getBlockClass()
        {
            return LongArrayBlock.class;
        }

        @Override
        public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
        {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
        {
            throw new RuntimeException("Not implemented");
        }
    }

    private static class YBlockEncoding
            implements BlockEncoding
    {
        @Override
        public String getName()
        {
            return "Y";
        }

        @Override
        public Class<? extends Block> getBlockClass()
        {
            return LongArrayBlock.class;
        }

        @Override
        public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
        {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
        {
            throw new RuntimeException("Not implemented");
        }
    }
}
