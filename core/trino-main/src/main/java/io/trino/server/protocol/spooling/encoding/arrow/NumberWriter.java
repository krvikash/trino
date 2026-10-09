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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.TrinoNumber;
import org.apache.arrow.vector.VarCharVector;

import static io.trino.spi.type.NumberType.NUMBER;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Writes {@link NumberType} values as decimal text.
 *
 * <p>The block holds a header and a big-endian magnitude rather than digits, and Arrow has no
 * arbitrary-precision decimal — its decimals are fixed at 128 or 256 bits, which would not
 * round-trip every value. Text is what the JSON protocol sends for this type as well, so the two
 * encodings hand the client the same string.
 *
 * <p>Unlike the other variable-width writers this cannot bulk-copy the block: every value has to
 * be converted, so it costs a {@code BigDecimal} and a string per row.
 */
public final class NumberWriter
        extends VariableWidthWriter<VarCharVector>
{
    public NumberWriter(VarCharVector vector)
    {
        super(vector);
    }

    @Override
    protected void writeValues(Block block)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            if (block.isNull(position)) {
                vector.setNull(position);
            }
            else {
                byte[] text = ((TrinoNumber) NUMBER.getObject(block, position)).toString().getBytes(UTF_8);
                vector.setSafe(position, text);
            }
        }
        vector.setValueCount(positionCount);
    }

    @Override
    protected long estimatedDataBufferSizeInBytes(Block block)
    {
        // An upper bound on the text length. Each byte of the big-endian unscaled value prints as at
        // most log10(256) ~ 2.41 digits. The block size also counts a 2-byte header per value and 5
        // bytes per position for offsets and nulls, which are not part of the unscaled value. On top
        // of the digits, a value needs at most 9 chars for sign, decimal point and exponent
        // ("E-15801") or leading zeros ("0.00000"), plus 1 for rounding the digit count up. Values
        // with no unscaled value are covered by that constant ("-Infinity" is 9 chars). Null
        // positions have no header, so unscaledBytes undercounts by 2 each, but the per-position
        // constant keeps the total positive.
        int positionCount = block.getPositionCount();
        long overheadBytes = 7L * positionCount;
        long unscaledBytes = block.getSizeInBytes() - overheadBytes;
        long extraCharactersEstimate = 10L * positionCount;
        return (long) Math.ceil(2.41 * unscaledBytes) + extraCharactersEstimate;
    }
}
