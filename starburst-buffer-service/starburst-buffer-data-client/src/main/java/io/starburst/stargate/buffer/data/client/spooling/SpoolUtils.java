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
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
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

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    public static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    public static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private SpoolUtils() {}
}
