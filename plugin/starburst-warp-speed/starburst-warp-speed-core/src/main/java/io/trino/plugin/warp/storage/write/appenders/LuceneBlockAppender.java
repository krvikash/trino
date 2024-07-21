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

import io.airlift.slice.Slice;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneIndexer;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.plugin.warp.warmup.exceptions.WarmupException;

import java.io.IOException;

public class LuceneBlockAppender
        extends CrcBlockAppender
{
    private final LuceneIndexer luceneIndexer;

    public LuceneBlockAppender(WriteJuffersWarmUpElement juffersWE,
            LuceneIndexer luceneIndexer)
    {
        super(juffersWE);
        this.luceneIndexer = luceneIndexer;
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos,
            BlockPosHolder blockPos,
            WarmUpElement warmUpElement,
            WarmupElementStatsBuilder warmupElementStatsBuilder,
            byte[] chunkHeader)
    {
        // TODO: handle arrays (VDB-2828)

        int nullsCount = 0;
        try {
            if (blockPos.mayHaveNull()) {
                while (blockPos.inRange()) {
                    if (blockPos.isNull()) {
                        nullsCount++;
                        nullBuff.put(NULL_VALUE_BYTE_SIGNAL);
                        luceneIndexer.addDoc(LuceneIndexer.LUCENE_NULL_STRING);
                    }
                    else {
                        nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                        writeValue(blockPos);
                    }
                    blockPos.advance();
                }
            }
            else {
                while (blockPos.inRange()) {
                    nullBuff.put(NON_NULL_VALUE_BYTE_SIGNAL);
                    writeValue(blockPos);
                    blockPos.advance();
                }
            }
        }
        catch (Exception e) {
            throw new WarmupException(
                    String.format("failed indexing value at position %d", blockPos.getPos()),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
        return new AppendResult(nullsCount);
    }

    private void writeValue(BlockPosHolder blockPos)
            throws IOException
    {
        Slice value = blockPos.getSlice();
        luceneIndexer.addDoc(value);
        juffersWE.updateLuceneProps(value);
    }
}
