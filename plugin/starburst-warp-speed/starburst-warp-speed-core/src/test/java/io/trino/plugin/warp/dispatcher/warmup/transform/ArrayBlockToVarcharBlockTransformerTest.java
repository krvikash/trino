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
package io.trino.plugin.warp.dispatcher.warmup.transform;

import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.plugin.warp.storage.common.StorageConstants.DONT_HAVE_NULL;
import static io.trino.plugin.warp.storage.common.StorageConstants.MAY_HAVE_NULL;
import static io.trino.plugin.warp.storage.common.StorageConstants.NOT_NULL_MARKER_VALUE;
import static io.trino.plugin.warp.storage.common.StorageConstants.NULL_MARKER_VALUE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class ArrayBlockToVarcharBlockTransformerTest
{
    private static final ArrayType BOOLEAN_ARRAY = new ArrayType(BOOLEAN);

    @Test
    void testBooleanArrayWithoutNulls()
    {
        Block converted = transform(List.of(
                List.of(true, false, true),
                List.of(),
                List.of(false)));

        assertThat(converted.getPositionCount()).isEqualTo(3);
        assertThat(VARCHAR.getSlice(converted, 0).getBytes()).containsExactly(DONT_HAVE_NULL, 1, 0, 1);
        assertThat(VARCHAR.getSlice(converted, 1).getBytes()).containsExactly(DONT_HAVE_NULL);
        assertThat(VARCHAR.getSlice(converted, 2).getBytes()).containsExactly(DONT_HAVE_NULL, 0);
    }

    @Test
    void testBooleanArrayWithNulls()
    {
        Block converted = transform(List.of(Arrays.asList(true, null, false)));

        assertThat(converted.getPositionCount()).isEqualTo(1);
        // header, then (null marker, value) per element; a null element carries a zero placeholder
        assertThat(VARCHAR.getSlice(converted, 0).getBytes()).containsExactly(
                MAY_HAVE_NULL,
                NOT_NULL_MARKER_VALUE,
                1,
                NULL_MARKER_VALUE,
                0,
                NOT_NULL_MARKER_VALUE,
                0);
    }

    private static Block transform(List<List<Boolean>> arrays)
    {
        BlockBuilder blockBuilder = BOOLEAN_ARRAY.createBlockBuilder(null, arrays.size());
        for (List<Boolean> array : arrays) {
            BOOLEAN_ARRAY.writeObject(blockBuilder, createBooleansBlock(array));
        }
        Block block = blockBuilder.build();

        BlockPosHolder blockPos = new BlockPosHolder(block, BOOLEAN_ARRAY, 0, block.getPositionCount());
        return new ArrayBlockToVarcharBlockTransformer().transformBlock(blockPos, BOOLEAN_ARRAY).getBlock();
    }
}
