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
package io.trino.plugin.warp.dispatcher.warmup.warmers;

import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.storage.write.PageSink;

import java.util.Arrays;
import java.util.Objects;

public final class WarmingCandidate
{
    private final long[] fileCookie;
    private final PageSink pageSink;
    private final int fileOffset;
    private final WarmupElementWriteMetadata warmupElementWriteMetadata;
    private final DictionaryWarmInfo dictionaryWarmInfo;
    private final RowGroupKey tmpRowGroupKey;
    private boolean isFailedCandidate;

    public WarmingCandidate(
            long[] fileCookie,
            PageSink pageSink,
            int fileOffset,
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            DictionaryWarmInfo dictionaryWarmInfo,
            RowGroupKey tmpRowGroupKey)
    {
        this.fileCookie = fileCookie;
        this.pageSink = pageSink;
        this.fileOffset = fileOffset;
        this.warmupElementWriteMetadata = warmupElementWriteMetadata;
        this.dictionaryWarmInfo = dictionaryWarmInfo;
        this.tmpRowGroupKey = tmpRowGroupKey;
        this.isFailedCandidate = false;
    }

    public long[] fileCookie()
    {
        return fileCookie;
    }

    public PageSink pageSink()
    {
        return pageSink;
    }

    public int fileOffset()
    {
        return fileOffset;
    }

    public WarmupElementWriteMetadata warmupElementWriteMetadata()
    {
        return warmupElementWriteMetadata;
    }

    public DictionaryWarmInfo getDictionaryWarmInfos()
    {
        return dictionaryWarmInfo;
    }

    public RowGroupKey tmpRowGroupKey()
    {
        return tmpRowGroupKey;
    }

    public void setFailedCandidate()
    {
        isFailedCandidate = true;
    }

    public boolean isFailedCandidate()
    {
        return isFailedCandidate;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (WarmingCandidate) obj;
        return Arrays.equals(this.fileCookie, that.fileCookie) &&
                Objects.equals(this.pageSink, that.pageSink) &&
                this.fileOffset == that.fileOffset &&
                this.isFailedCandidate == that.isFailedCandidate &&
                Objects.equals(this.warmupElementWriteMetadata, that.warmupElementWriteMetadata) &&
                Objects.equals(this.dictionaryWarmInfo, that.dictionaryWarmInfo) &&
                Objects.equals(this.tmpRowGroupKey, that.tmpRowGroupKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(fileCookie), pageSink, fileOffset, isFailedCandidate, warmupElementWriteMetadata, dictionaryWarmInfo, tmpRowGroupKey);
    }

    @Override
    public String toString()
    {
        return "WarmingCandidate[" +
                "fileCookie=" + Arrays.toString(fileCookie) + ", " +
                "pageSink=" + pageSink + ", " +
                "fileOffset=" + fileOffset + ", " +
                "warmupElementWriteMetadata=" + warmupElementWriteMetadata + ", " +
                "dictionaryWarmInfo=" + dictionaryWarmInfo + ", " +
                "isFailedCandidate=" + isFailedCandidate + ", " +
                "tmpRowGroupKey=" + tmpRowGroupKey + ']';
    }
}
