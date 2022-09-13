/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;

import static io.airlift.slice.UnsafeSlice.getByteUnchecked;
import static io.airlift.slice.UnsafeSlice.getIntUnchecked;
import static io.airlift.slice.UnsafeSlice.getShortUnchecked;
import static java.util.Objects.requireNonNull;

public final class PagesSerdeUtil
{
    public static final int DATA_PAGE_HEADER_SIZE = Short.BYTES + Byte.BYTES + Integer.BYTES; // taskId, attemptId and data size

    private PagesSerdeUtil() {}

    /**
     * Special checksum value used to verify configuration consistency across nodes (all nodes need to have data integrity configured the same way).
     * It's not just 0, so that hypothetical zero-ed out data is not treated as valid payload with no checksum.
     */
    public static final long NO_CHECKSUM = 0x0123456789abcdefL;

    public static long calculateChecksum(List<DataPage> dataPages)
    {
        XxHash64 hash = new XxHash64();
        for (DataPage dataPage : dataPages) {
            hash.update(dataPage.data());
        }
        long checksum = hash.hash();
        // Since NO_CHECKSUM is assigned a special meaning, it is not a valid checksum.
        if (checksum == NO_CHECKSUM) {
            return checksum + 1;
        }
        return checksum;
    }

    public static Iterator<DataPage> readSerializedPages(InputStream inputStream)
    {
        return new SerializedPageReader(inputStream);
    }

    private static class SerializedPageReader
            extends AbstractIterator<DataPage>
    {
        private static final int ATTEMPT_ID_OFFSET = Short.BYTES;
        private static final int PAGE_SIZE_OFFSET = Short.BYTES + Byte.BYTES;
        private static final int HEADER_SIZE = Short.BYTES + Byte.BYTES + Integer.BYTES; // taskId, attemptId and data size

        private final InputStream inputStream;
        private final byte[] headerBuffer = new byte[HEADER_SIZE];
        private final Slice headerSlice = Slices.wrappedBuffer(headerBuffer);

        SerializedPageReader(InputStream input)
        {
            this.inputStream = requireNonNull(input, "inputStream is null");
        }

        @Override
        protected DataPage computeNext()
        {
            try {
                int read = ByteStreams.read(inputStream, headerBuffer, 0, headerBuffer.length);
                if (read <= 0) {
                    return endOfData();
                }
                if (read != headerBuffer.length) {
                    throw new EOFException();
                }

                int taskId = getShortUnchecked(headerSlice, 0);
                int attemptId = getByteUnchecked(headerSlice, ATTEMPT_ID_OFFSET);
                int pageSize = getIntUnchecked(headerSlice, PAGE_SIZE_OFFSET);
                Slice data = Slices.allocate(pageSize);
                data.getOutput().writeBytes(inputStream, pageSize);
                return new DataPage(taskId, attemptId, data);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
