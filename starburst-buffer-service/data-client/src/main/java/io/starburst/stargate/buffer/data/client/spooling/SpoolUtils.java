/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling;

import com.google.common.collect.ImmutableList;
import com.google.common.io.LittleEndianDataInputStream;
import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.DataPage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.readSerializedPages;
import static java.lang.String.format;

public final class SpoolUtils
{
    public static final String PATH_SEPARATOR = "/";
    public static final int CHUNK_FILE_HEADER_SIZE = Long.BYTES + Integer.BYTES; // checksum, numDataPages

    public static List<DataPage> toDataPages(LittleEndianDataInputStream input, boolean dataIntegrityVerificationEnabled)
    {
        try {
            long checksum = input.readLong();
            int pagesCount = input.readInt();
            List<DataPage> pages = ImmutableList.copyOf(readSerializedPages(input));
            verifyChecksum(checksum, pages, dataIntegrityVerificationEnabled);
            if (pages.size() != pagesCount) {
                throw new DataApiException(INTERNAL_ERROR, "Wrong number of pages, expected %s, but read %s".formatted(pagesCount, pages.size()));
            }
            return pages;
        }
        catch (IOException e) {
            throw new DataApiException(INTERNAL_ERROR, "IOException", e);
        }
    }

    public static List<DataPage> toDataPages(byte[] bytes, boolean dataIntegrityVerificationEnabled)
    {
        return toDataPages(new LittleEndianDataInputStream(new ByteArrayInputStream(bytes)), dataIntegrityVerificationEnabled);
    }

    private static void verifyChecksum(long readChecksum, List<DataPage> pages, boolean dataIntegrityVerificationEnabled)
    {
        if (dataIntegrityVerificationEnabled) {
            long calculatedChecksum = calculateChecksum(pages);
            if (readChecksum != calculatedChecksum) {
                throw new DataApiException(INTERNAL_ERROR, format("Data corruption, read checksum: 0x%08x, calculated checksum: 0x%08x", readChecksum, calculatedChecksum));
            }
        }
        else {
            if (readChecksum != NO_CHECKSUM) {
                throw new DataApiException(INTERNAL_ERROR, format("Expected checksum to be NO_CHECKSUM (0x%08x) but is 0x%08x", NO_CHECKSUM, readChecksum));
            }
        }
    }

    private SpoolUtils() {}
}
