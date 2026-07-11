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

import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.plugin.warp.gen.constants.QueryResultType.QUERY_RESULT_TYPE_RAW;
import static io.trino.plugin.warp.gen.constants.QueryResultType.QUERY_RESULT_TYPE_RAW_NO_NULL;
import static io.trino.plugin.warp.gen.constants.QueryResultType.QUERY_RESULT_TYPE_SINGLE;
import static io.trino.plugin.warp.gen.constants.QueryResultType.QUERY_RESULT_TYPE_SINGLE_NO_NULL;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_BOOLEAN;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

class BooleanBlockFillerTest
{
    // BooleanBlockAppender stores one byte per value; a null row carries a zero placeholder in the
    // record buffer and is flagged in the separate null buffer
    private static final byte TRUE = 1;
    private static final byte FALSE = 0;
    private static final byte NULL = -1;
    private static final byte NOT_NULL = 0;

    @Test
    void testFillRawBlock()
            throws IOException
    {
        Block block = fill(QUERY_RESULT_TYPE_RAW_NO_NULL, 5, new byte[] {TRUE, FALSE, TRUE, TRUE, FALSE}, null);

        assertThat(block).isInstanceOf(BitArrayBlock.class);
        assertBlockEquals(BOOLEAN, block, createBooleansBlock(true, false, true, true, false));
    }

    @Test
    void testFillRawBlockAcrossWordBoundary()
            throws IOException
    {
        // more rows than a single bitmap word holds, so values must land in the second word
        int rows = 70;
        byte[] records = new byte[rows];
        Boolean[] expected = new Boolean[rows];
        for (int row = 0; row < rows; row++) {
            boolean value = row % 3 == 0;
            records[row] = value ? TRUE : FALSE;
            expected[row] = value;
        }

        Block block = fill(QUERY_RESULT_TYPE_RAW_NO_NULL, rows, records, null);

        assertThat(block).isInstanceOf(BitArrayBlock.class);
        assertBlockEquals(BOOLEAN, block, createBooleansBlock(expected));
    }

    @Test
    void testFillRawBlockWithNulls()
            throws IOException
    {
        Block block = fill(
                QUERY_RESULT_TYPE_RAW,
                4,
                new byte[] {TRUE, FALSE, FALSE, FALSE},
                new byte[] {NOT_NULL, NULL, NOT_NULL, NULL});

        assertThat(block).isInstanceOf(BitArrayBlock.class);
        assertBlockEquals(BOOLEAN, block, createBooleansBlock(true, null, false, null));
    }

    @Test
    void testFillSingleValueWithNulls()
            throws IOException
    {
        Block block = fill(QUERY_RESULT_TYPE_SINGLE, 3, new byte[] {TRUE}, new byte[] {NOT_NULL, NULL, NOT_NULL});

        assertThat(block).isInstanceOf(DictionaryBlock.class);
        assertBlockEquals(BOOLEAN, block, createBooleansBlock(true, null, true));
    }

    @Test
    void testFillSingleValue()
            throws IOException
    {
        Block block = fill(QUERY_RESULT_TYPE_SINGLE_NO_NULL, 3, new byte[] {FALSE}, null);

        assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        assertBlockEquals(BOOLEAN, block, createBooleansBlock(false, 3));
    }

    // nulls == null means the storage engine reported no nulls, so the filler must not touch the null buffer
    private static Block fill(QueryResultType queryResultType, int rowsToFill, byte[] records, byte[] nulls)
            throws IOException
    {
        ReadJuffersWarmUpElement juffers = new ReadJuffersWarmUpElement()
        {
            @Override
            public Buffer getRecordBuffer()
            {
                return ByteBuffer.wrap(records);
            }

            @Override
            public ByteBuffer getNullBuffer()
            {
                return ByteBuffer.wrap(nulls);
            }
        };
        return new BooleanBlockFiller().fillBlock(juffers, queryResultType, rowsToFill, REC_TYPE_BOOLEAN, Byte.BYTES, nulls != null);
    }
}
