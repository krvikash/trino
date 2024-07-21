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

import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;

public class CrcMapBlockAppender
        extends CrcBlockAppender

{
    private final BlockAppender valuesBlockAppender;

    public CrcMapBlockAppender(WriteJuffersWarmUpElement juffersWE, BlockAppender valuesBlockAppender)
    {
        super(juffersWE);
        this.valuesBlockAppender = valuesBlockAppender;
    }

    @Override
    AppendResult appendWithoutDictionary(int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            byte[] chunkHeader)
    {
        TransformedColumn transformedColumn = (TransformedColumn) warmUpElement.getWarpColumn();
        WarpConstant key = transformedColumn.getTransformFunction().transformParams().getFirst();
        return valuesBlockAppender.appendFromMapBlock(blockPos, jufferPos, key.getValue());
    }
}
