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
package io.trino.plugin.iceberg.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.VariableWidthBlock;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats.blockToByteBuffer;
import static io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats.varcharBlockToByteBuffer;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergThetaSketchForStats
{
    @Test
    void testVarcharBlockToByteBuffer()
    {
        for (String value : ImmutableList.of("abc_def ", " abc^_def_", "abc\u007f\u0123\udbfe", "abc\u0123\ud83d\ude80def~\u007f\u00ff\u0123\uccf0%")) {
            verifyToByteBuffer(value);
        }
    }

    private static void verifyToByteBuffer(String value)
    {
        Slice slice = Slices.utf8Slice(value);
        VariableWidthBlock block = new VariableWidthBlock(1, slice, new int[] {0, slice.length()}, Optional.empty());
        assertThat(varcharBlockToByteBuffer(block, 0))
                .isEqualTo(blockToByteBuffer(VARCHAR, block, 0));
    }
}
