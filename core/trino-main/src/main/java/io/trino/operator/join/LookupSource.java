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
package io.trino.operator.join;

import io.trino.annotation.NotThreadSafe;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.PreSizedBlockBuilder;

import java.io.Closeable;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    long JOIN_POSITION_NOT_FOUND = -1L;

    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    /**
     * The `rawHashes` and `result` arrays are global to the entire processed page (thus, the same size),
     * while the `positions` array may hold any number of selected positions from this page
     */
    default void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        for (int i = 0; i < positions.length; i++) {
            result[positions[i]] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage, rawHashes[positions[i]]);
        }
    }

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Appends the build-side row at {@code position} into the provided block builders.
     * {@code position} is always a real match, never {@link #JOIN_POSITION_NOT_FOUND}.
     * Default implementation throws; only sources used by the non-spilling join implement it.
     */
    default void appendTo(long position, PreSizedBlockBuilder[] builders)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Batched variant of {@link #appendTo(long, PreSizedBlockBuilder[])}.
     * Positions equal to {@link #JOIN_POSITION_NOT_FOUND} produce nulls in every builder.
     */
    default void appendTo(long[] positions, int offset, int length, PreSizedBlockBuilder[] builders)
    {
        for (int i = 0; i < length; i++) {
            long position = positions[offset + i];
            if (position == JOIN_POSITION_NOT_FOUND) {
                for (PreSizedBlockBuilder builder : builders) {
                    builder.appendNull();
                }
            }
            else {
                appendTo(position, builders);
            }
        }
    }

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    @Override
    void close();
}
