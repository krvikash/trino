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
package io.trino.plugin.warp.storage.write.appenders;

import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.plugin.warp.warmup.exceptions.WarmupException;

import java.util.Locale;
import java.util.function.Function;

public class TransformedCrcDateBlockAppender
        extends CrcIntBlockAppender
{
    private final Function<BlockPosHolder, Integer> transformColumn;

    public TransformedCrcDateBlockAppender(
            WriteJuffersWarmUpElement juffersWE,
            Function<BlockPosHolder, Integer> transformColumnFunction)
    {
        super(juffersWE);
        this.transformColumn = transformColumnFunction;
    }

    // the source block is varchar, so values go through the row-by-row transform instead of the typed int paths
    @Override
    public AppendResult appendValues(
            int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder)
    {
        int nullsCount = 0;
        if (blockPos.mayHaveNull()) {
            for (; blockPos.inRange(); blockPos.advance()) {
                if (blockPos.isNull()) {
                    nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                    nullsCount++;
                }
                else {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    int val = transformedValue(blockPos);
                    warmupElementStatsBuilder.updateMinMax(val);
                    writeValue(jufferPos, blockPos, val);
                }
            }
        }
        else {
            for (; blockPos.inRange(); blockPos.advance()) {
                nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                int val = transformedValue(blockPos);
                warmupElementStatsBuilder.updateMinMax(val);
                writeValue(jufferPos, blockPos, val);
            }
        }
        return new AppendResult(nullsCount);
    }

    private int transformedValue(BlockPosHolder blockPos)
    {
        try {
            return transformColumn.apply(blockPos);
        }
        catch (Exception e) {
            String invalidSlice = blockPos.getSlice().toStringUtf8();
            throw new WarmupException(
                    String.format(Locale.US, "failed to transform %s into date format", invalidSlice),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
    }
}
