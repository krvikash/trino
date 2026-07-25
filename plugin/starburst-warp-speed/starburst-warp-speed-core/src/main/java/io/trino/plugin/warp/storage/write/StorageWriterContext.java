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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneIndexer;
import io.trino.plugin.warp.storage.write.appenders.BlockAppender;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

final class StorageWriterContext
{
    private final AtomicReference<Object> isCleanupDone;
    private final WarmupElementStatsBuilder warmupElementStatsBuilder;
    private boolean weClosed;
    private final WarmupElementWriteMetadata warmupElementWriteMetadata;
    private int recordBufferSize;
    private int recordBufferPos;
    private WarmUpElement.Builder warmupElementBuilder;
    private final WriteJuffersWarmUpElement writeJuffersWarmUpElement;
    private final WarmUpState warmUpState;
    private final BlockAppender blockAppender;
    private boolean weSuccess;
    private final Optional<LuceneIndexer> luceneIndexer;
    private final byte warmId;

    StorageWriterContext(
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            WarmUpElement.Builder warmupElementBuilder,
            WriteJuffersWarmUpElement writeJuffersWarmUpElement,
            WarmUpState warmUpState,
            BlockAppender blockAppender,
            Optional<LuceneIndexer> luceneIndexer,
            byte warmId)
    {
        this.warmupElementWriteMetadata = warmupElementWriteMetadata;
        this.recordBufferSize = 0;
        this.recordBufferPos = 0;
        this.warmupElementBuilder = warmupElementBuilder;
        this.writeJuffersWarmUpElement = writeJuffersWarmUpElement;
        this.warmUpState = warmUpState;
        this.blockAppender = blockAppender;
        this.weSuccess = true;
        this.luceneIndexer = luceneIndexer;
        this.warmId = warmId;
        this.isCleanupDone = new AtomicReference<>(null);
        this.warmupElementStatsBuilder = new WarmupElementStatsBuilder(warmupElementWriteMetadata.warmUpElement().getWarmupElementStats());
    }

    int getRecordBufferPos()
    {
        return recordBufferPos;
    }

    void setFailed()
    {
        weSuccess = false;
    }

    void setWeClosed()
    {
        weClosed = true;
    }

    public WarmUpState getWarmUpState()
    {
        return warmUpState;
    }

    int getRecordBufferSize()
    {
        return recordBufferSize;
    }

    boolean isWeClosed()
    {
        return weClosed;
    }

    WarmUpElement.Builder getWarmupElementBuilder()
    {
        return warmupElementBuilder;
    }

    WriteJuffersWarmUpElement getWriteJuffersWarmUpElement()
    {
        return writeJuffersWarmUpElement;
    }

    Optional<LuceneIndexer> getLuceneIndexer()
    {
        return luceneIndexer;
    }

    boolean weSuccess()
    {
        return weSuccess;
    }

    void setWarmupElementBuilder(WarmUpElement.Builder warmupElementBuilder)
    {
        this.warmupElementBuilder = warmupElementBuilder;
    }

    void resetRecords()
    {
        this.recordBufferSize = 0;
        this.recordBufferPos = 0;
    }

    void resetRecordBufferPos()
    {
        recordBufferPos = 0;
    }

    void incRecordBufferPos(int nCurrentRows)
    {
        recordBufferPos += nCurrentRows;
    }

    boolean isRecordBufferFull()
    {
        return (recordBufferSize > 0) && (recordBufferPos >= recordBufferSize);
    }

    int getRemainingBufferSize()
    {
        return recordBufferSize - recordBufferPos;
    }

    void setRecordBufferSize(int newSize)
    {
        this.recordBufferSize = newSize;
    }

    WarmupElementWriteMetadata getWarmupElementWriteMetadata()
    {
        return warmupElementWriteMetadata;
    }

    BlockAppender getBlockAppender()
    {
        return blockAppender;
    }

    WarmupElementStatsBuilder getWarmupElementStatsBuilder()
    {
        return warmupElementStatsBuilder;
    }

    AtomicReference<Object> getIsCleanupDone()
    {
        return isCleanupDone;
    }

    byte getWarmId()
    {
        return warmId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StorageWriterContext that = (StorageWriterContext) o;
        return weClosed == that.weClosed &&
                recordBufferSize == that.recordBufferSize &&
                recordBufferPos == that.recordBufferPos &&
                weSuccess == that.weSuccess &&
                Objects.equals(warmupElementWriteMetadata, that.warmupElementWriteMetadata) &&
                Objects.equals(warmupElementBuilder, that.warmupElementBuilder) &&
                Objects.equals(writeJuffersWarmUpElement, that.writeJuffersWarmUpElement) &&
                Objects.equals(blockAppender, that.blockAppender) &&
                Objects.equals(luceneIndexer, that.luceneIndexer);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                weClosed,
                warmupElementWriteMetadata,
                recordBufferSize,
                recordBufferPos,
                warmupElementBuilder,
                writeJuffersWarmUpElement,
                blockAppender,
                weSuccess,
                luceneIndexer);
    }

    @Override
    public String toString()
    {
        return "StorageWriterParams{" +
                "isCleanupDone=" + isCleanupDone +
                ", weClosed=" + weClosed +
                ", warmupElementWriteMetadata=" + warmupElementWriteMetadata +
                ", requestedRecordBufferSize=" + recordBufferSize +
                ", recordBufferPos=" + recordBufferPos +
                ", warmupElementBuilder=" + warmupElementBuilder +
                ", writeJuffersWarmUpElement=" + writeJuffersWarmUpElement +
                ", blockAppender=" + blockAppender +
                ", weSuccess=" + weSuccess +
                ", luceneIndexer=" + luceneIndexer +
                '}';
    }
}
