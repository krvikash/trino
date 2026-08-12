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
package com.epam.parso.impl;

import com.epam.parso.Column;
import com.epam.parso.ColumnFormat;
import com.epam.parso.ColumnMissingInfo;
import com.epam.parso.SasFileProperties;
import com.epam.parso.date.OutputDateType;
import com.epam.parso.date.SasTemporalFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.epam.parso.impl.ParserMessageConstants.BLOCK_COUNT;
import static com.epam.parso.impl.ParserMessageConstants.COLUMN_FORMAT;
import static com.epam.parso.impl.ParserMessageConstants.EMPTY_INPUT_STREAM;
import static com.epam.parso.impl.ParserMessageConstants.FILE_NOT_VALID;
import static com.epam.parso.impl.ParserMessageConstants.NO_SUPPORTED_COMPRESSION_LITERAL;
import static com.epam.parso.impl.ParserMessageConstants.NULL_COMPRESSION_LITERAL;
import static com.epam.parso.impl.ParserMessageConstants.PAGE_TYPE;
import static com.epam.parso.impl.ParserMessageConstants.SUBHEADER_COUNT;
import static com.epam.parso.impl.ParserMessageConstants.SUBHEADER_PROCESS_FUNCTION_NAME;
import static com.epam.parso.impl.ParserMessageConstants.UNKNOWN_SUBHEADER_SIGNATURE;
import static com.epam.parso.impl.SasFileConstants.ALIGN_1_CHECKER_VALUE;
import static com.epam.parso.impl.SasFileConstants.ALIGN_1_LENGTH;
import static com.epam.parso.impl.SasFileConstants.ALIGN_1_OFFSET;
import static com.epam.parso.impl.SasFileConstants.ALIGN_1_VALUE;
import static com.epam.parso.impl.SasFileConstants.ALIGN_2_LENGTH;
import static com.epam.parso.impl.SasFileConstants.ALIGN_2_OFFSET;
import static com.epam.parso.impl.SasFileConstants.ALIGN_2_VALUE;
import static com.epam.parso.impl.SasFileConstants.BIG_ENDIAN_CHECKER;
import static com.epam.parso.impl.SasFileConstants.BITS_IN_BYTE;
import static com.epam.parso.impl.SasFileConstants.BLOCK_COUNT_LENGTH;
import static com.epam.parso.impl.SasFileConstants.BLOCK_COUNT_OFFSET;
import static com.epam.parso.impl.SasFileConstants.BYTES_IN_DOUBLE;
import static com.epam.parso.impl.SasFileConstants.BYTES_IN_INT;
import static com.epam.parso.impl.SasFileConstants.BYTES_IN_LONG;
import static com.epam.parso.impl.SasFileConstants.COLUMN_DATA_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_DATA_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_DATA_OFFSET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_OFFSET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_PRECISION_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_PRECISION_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_WIDTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_FORMAT_WIDTH_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_OFFSET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_OFFSET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_POINTER_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_TEXT_SUBHEADER_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_NAME_TEXT_SUBHEADER_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COLUMN_TYPE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COLUMN_TYPE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COMPRESSED_SUBHEADER_ID;
import static com.epam.parso.impl.SasFileConstants.COMPRESSED_SUBHEADER_TYPE;
import static com.epam.parso.impl.SasFileConstants.COMPRESSION_METHOD_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COMPRESSION_METHOD_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COMPRESSION_METHOD_OFFSET;
import static com.epam.parso.impl.SasFileConstants.COMPRESSION_METHOD_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.COMPRESS_BIN_IDENTIFYING_STRING;
import static com.epam.parso.impl.SasFileConstants.COMPRESS_CHAR_IDENTIFYING_STRING;
import static com.epam.parso.impl.SasFileConstants.DATASET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.DATASET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.DATE_CREATED_LENGTH;
import static com.epam.parso.impl.SasFileConstants.DATE_CREATED_OFFSET;
import static com.epam.parso.impl.SasFileConstants.DATE_MODIFIED_LENGTH;
import static com.epam.parso.impl.SasFileConstants.DATE_MODIFIED_OFFSET;
import static com.epam.parso.impl.SasFileConstants.DELETED_ROW_COUNT_OFFSET_MULTIPLIER;
import static com.epam.parso.impl.SasFileConstants.ENCODING_LENGTH;
import static com.epam.parso.impl.SasFileConstants.ENCODING_OFFSET;
import static com.epam.parso.impl.SasFileConstants.ENDIANNESS_LENGTH;
import static com.epam.parso.impl.SasFileConstants.ENDIANNESS_OFFSET;
import static com.epam.parso.impl.SasFileConstants.EPSILON;
import static com.epam.parso.impl.SasFileConstants.FILE_FORMAT_LENGTH_LENGTH;
import static com.epam.parso.impl.SasFileConstants.FILE_FORMAT_LENGTH_OFFSET;
import static com.epam.parso.impl.SasFileConstants.FILE_FORMAT_OFFSET_LENGTH;
import static com.epam.parso.impl.SasFileConstants.FILE_FORMAT_OFFSET_OFFSET;
import static com.epam.parso.impl.SasFileConstants.FILE_TYPE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.FILE_TYPE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.HEADER_SIZE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.HEADER_SIZE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.LITTLE_ENDIAN_CHECKER;
import static com.epam.parso.impl.SasFileConstants.NAN_EPSILON;
import static com.epam.parso.impl.SasFileConstants.OS_MAKER_LENGTH;
import static com.epam.parso.impl.SasFileConstants.OS_MAKER_OFFSET;
import static com.epam.parso.impl.SasFileConstants.OS_NAME_LENGTH;
import static com.epam.parso.impl.SasFileConstants.OS_NAME_OFFSET;
import static com.epam.parso.impl.SasFileConstants.OS_VERSION_NUMBER_LENGTH;
import static com.epam.parso.impl.SasFileConstants.OS_VERSION_NUMBER_OFFSET;
import static com.epam.parso.impl.SasFileConstants.PAGE_BIT_OFFSET_X64;
import static com.epam.parso.impl.SasFileConstants.PAGE_BIT_OFFSET_X86;
import static com.epam.parso.impl.SasFileConstants.PAGE_CMETA_TYPE;
import static com.epam.parso.impl.SasFileConstants.PAGE_COUNT_LENGTH;
import static com.epam.parso.impl.SasFileConstants.PAGE_COUNT_OFFSET;
import static com.epam.parso.impl.SasFileConstants.PAGE_DATA_TYPE;
import static com.epam.parso.impl.SasFileConstants.PAGE_DATA_TYPE_2;
import static com.epam.parso.impl.SasFileConstants.PAGE_DELETED_POINTER_LENGTH;
import static com.epam.parso.impl.SasFileConstants.PAGE_DELETED_POINTER_OFFSET_X64;
import static com.epam.parso.impl.SasFileConstants.PAGE_DELETED_POINTER_OFFSET_X86;
import static com.epam.parso.impl.SasFileConstants.PAGE_META_TYPE_1;
import static com.epam.parso.impl.SasFileConstants.PAGE_META_TYPE_2;
import static com.epam.parso.impl.SasFileConstants.PAGE_MIX_TYPE_1;
import static com.epam.parso.impl.SasFileConstants.PAGE_MIX_TYPE_2;
import static com.epam.parso.impl.SasFileConstants.PAGE_SIZE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.PAGE_SIZE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.PAGE_TYPE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.PAGE_TYPE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.ROW_COUNT_OFFSET_MULTIPLIER;
import static com.epam.parso.impl.SasFileConstants.ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER;
import static com.epam.parso.impl.SasFileConstants.ROW_LENGTH_OFFSET_MULTIPLIER;
import static com.epam.parso.impl.SasFileConstants.SAS_CHARACTER_ENCODINGS;
import static com.epam.parso.impl.SasFileConstants.SAS_RELEASE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.SAS_RELEASE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.SAS_SERVER_TYPE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.SAS_SERVER_TYPE_OFFSET;
import static com.epam.parso.impl.SasFileConstants.SUBHEADER_COUNT_LENGTH;
import static com.epam.parso.impl.SasFileConstants.SUBHEADER_COUNT_OFFSET;
import static com.epam.parso.impl.SasFileConstants.SUBHEADER_POINTERS_OFFSET;
import static com.epam.parso.impl.SasFileConstants.SUBHEADER_POINTER_LENGTH_X64;
import static com.epam.parso.impl.SasFileConstants.SUBHEADER_POINTER_LENGTH_X86;
import static com.epam.parso.impl.SasFileConstants.TEXT_BLOCK_SIZE_LENGTH;
import static com.epam.parso.impl.SasFileConstants.TRUNCATED_SUBHEADER_ID;
import static com.epam.parso.impl.SasFileConstants.U64_BYTE_CHECKER_VALUE;

// This class is a verbatim copy of com.epam.parso.impl.SasFileParser from the parso library
// (https://github.com/epam/parso), placed in the same package so it can access package-private
// constants. The only additions over the original are the elements that enable page-range reads
// for Trino split execution:
//
//   private long currentPageIndex — zero-based stream index of the page currently loaded
//   private long maxPageExclusive — end (exclusive) of this reader's page range, -1 = unbounded
//   setPageRange(long, long)      — called by SasRecordCursor to restrict the reader to a range
//   range guard in processNextPage — stops before consuming a page outside the range
//   empty-page-state guard in readNext (META branch) — returns null instead of failing when the
//     range or file ends before a page with data subheaders is found
//
// The right long-term fix is to contribute this capability upstream to parso or maintain a
// starburst-parso fork, so the copy can be removed. Until then, do not edit this file for
// anything other than keeping it in sync with the upstream SasFileParser.
@SuppressWarnings("all")
public final class CustomSasFileParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SasFileParser.class);

    private static final Map<Long, SubheaderIndexes> SUBHEADER_SIGNATURE_TO_INDEX;

    private static final Map<String, Decompressor> LITERALS_TO_DECOMPRESSOR = new HashMap<>();

    private static final int MAX_PAGE_LENGTH = 10000000;

    private static final byte[] SKIP_BYTE_BUFFER = new byte[4096];

    // TODO (parso fork): added for split-range reads — not present in upstream SasFileParser
    private long currentPageIndex = -1;
    private long maxPageExclusive = -1;

    static {
        Map<Long, SubheaderIndexes> tmpMap = new HashMap<>();
        tmpMap.put((long) 0xF7F7F7F7, SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX);
        tmpMap.put((long) 0xF6F6F6F6, SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFC00, SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFFFD, SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFFFF, SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFFFC, SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFBFE, SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX);
        tmpMap.put((long) 0xFFFFFFFE, SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX);
        tmpMap.put(0x00000000F7F7F7F7L, SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0x00000000F6F6F6F6L, SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0xF7F7F7F700000000L, SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0xF6F6F6F600000000L, SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0xF7F7F7F7FFFFFBFEL, SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0xF6F6F6F6FFFFFBFEL, SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX);
        tmpMap.put(0x00FCFFFFFFFFFFFFL, SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX);
        tmpMap.put(0xFDFFFFFFFFFFFFFFL, SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX);
        tmpMap.put(0xFFFFFFFFFFFFFFFFL, SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX);
        tmpMap.put(0xFCFFFFFFFFFFFFFFL, SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX);
        tmpMap.put(0xFEFBFFFFFFFFFFFFL, SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX);
        tmpMap.put(0xFEFFFFFFFFFFFFFFL, SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX);
        SUBHEADER_SIGNATURE_TO_INDEX = java.util.Collections.unmodifiableMap(tmpMap);
    }

    static {
        LITERALS_TO_DECOMPRESSOR.put(COMPRESS_CHAR_IDENTIFYING_STRING, CharDecompressor.INSTANCE);
        LITERALS_TO_DECOMPRESSOR.put(COMPRESS_BIN_IDENTIFYING_STRING, BinDecompressor.INSTANCE);
    }

    private final DataInputStream sasFileStream;

    private final Boolean byteOutput;

    private final OutputDateType outputDateType;

    private final List<SubheaderPointer> currentPageDataSubheaderPointers = new ArrayList<>();
    private final SasFileProperties sasFileProperties = new SasFileProperties();
    private final List<byte[]> columnsNamesBytes = new ArrayList<>();
    private final List<String> columnsNamesList = new ArrayList<>();
    private final List<Class<?>> columnsTypesList = new ArrayList<>();
    private final List<Long> columnsDataOffset = new ArrayList<>();
    private final List<Integer> columnsDataLength = new ArrayList<>();
    private final List<Column> columns = new ArrayList<>();
    private final Map<SubheaderIndexes, ProcessingSubheader> subheaderIndexToClass;
    private String encoding = "US-ASCII";
    private byte[] cachedPage;
    private int currentPageType;
    private int currentPageBlockCount;
    private int currentPageSubheadersCount;
    private int currentFilePosition;
    private int currentColumnNumber;
    private int currentRowInFileIndex;
    private int currentRowOnPageIndex;
    private Object[] currentRow;
    private boolean eof;

    private int fileLabelOffset;

    private int compressionMethodOffset;

    private int compressionMethodLength;

    private int fileLabelLength;

    private final List<ColumnMissingInfo> columnMissingInfoList = new ArrayList<>();

    private String deletedMarkers = "";

    private final SasTemporalFormatter sasTemporalFormatter = new SasTemporalFormatter();

    private CustomSasFileParser(Builder builder)
            throws IOException
    {
        sasFileStream = new DataInputStream(builder.sasFileStream);
        byteOutput = builder.byteOutput;
        outputDateType = builder.outputDateType;

        Map<SubheaderIndexes, ProcessingSubheader> tmpMap = new HashMap<>();
        tmpMap.put(SubheaderIndexes.ROW_SIZE_SUBHEADER_INDEX, new RowSizeSubheader());
        tmpMap.put(SubheaderIndexes.COLUMN_SIZE_SUBHEADER_INDEX, new ColumnSizeSubheader());
        tmpMap.put(SubheaderIndexes.SUBHEADER_COUNTS_SUBHEADER_INDEX, new SubheaderCountsSubheader());
        tmpMap.put(SubheaderIndexes.COLUMN_TEXT_SUBHEADER_INDEX, new ColumnTextSubheader());
        tmpMap.put(SubheaderIndexes.COLUMN_NAME_SUBHEADER_INDEX, new ColumnNameSubheader());
        tmpMap.put(SubheaderIndexes.COLUMN_ATTRIBUTES_SUBHEADER_INDEX, new ColumnAttributesSubheader());
        tmpMap.put(SubheaderIndexes.FORMAT_AND_LABEL_SUBHEADER_INDEX, new FormatAndLabelSubheader());
        tmpMap.put(SubheaderIndexes.COLUMN_LIST_SUBHEADER_INDEX, new ColumnListSubheader());
        tmpMap.put(SubheaderIndexes.DATA_SUBHEADER_INDEX, new DataSubheader());
        subheaderIndexToClass = java.util.Collections.unmodifiableMap(tmpMap);

        getMetadataFromSasFile(builder.encoding);
    }

    private void getMetadataFromSasFile(String encoding)
            throws IOException
    {
        boolean endOfMetadata = false;
        processSasFileHeader(encoding);
        cachedPage = new byte[sasFileProperties.getPageLength()];
        while (!endOfMetadata) {
            try {
                sasFileStream.readFully(cachedPage, 0, sasFileProperties.getPageLength());
            }
            catch (EOFException ex) {
                eof = true;
                break;
            }
            // TODO (parso fork): track the stream index of the loaded page for split-range reads
            currentPageIndex++;
            endOfMetadata = processSasFilePageMeta();
        }
    }

    private void processSasFileHeader(String builderEncoding)
            throws IOException
    {
        int align1 = 0;
        int align2 = 0;

        Long[] offsetForAlign = {ALIGN_1_OFFSET, ALIGN_2_OFFSET};
        Integer[] lengthForAlign = {ALIGN_1_LENGTH, ALIGN_2_LENGTH};
        List<byte[]> varsForAlign = getBytesFromFile(offsetForAlign, lengthForAlign);

        if (varsForAlign.get(0)[0] == U64_BYTE_CHECKER_VALUE) {
            align2 = ALIGN_2_VALUE;
            sasFileProperties.setU64(true);
        }

        if (varsForAlign.get(1)[0] == ALIGN_1_CHECKER_VALUE) {
            align1 = ALIGN_1_VALUE;
        }

        int totalAlign = align1 + align2;

        Long[] offset = {
                ENDIANNESS_OFFSET, ENCODING_OFFSET, DATASET_OFFSET, FILE_TYPE_OFFSET,
                DATE_CREATED_OFFSET + align1, DATE_MODIFIED_OFFSET + align1, HEADER_SIZE_OFFSET + align1,
                PAGE_SIZE_OFFSET + align1, PAGE_COUNT_OFFSET + align1, SAS_RELEASE_OFFSET + totalAlign,
                SAS_SERVER_TYPE_OFFSET + totalAlign, OS_VERSION_NUMBER_OFFSET + totalAlign,
                OS_MAKER_OFFSET + totalAlign, OS_NAME_OFFSET + totalAlign,
        };
        Integer[] length = {
                ENDIANNESS_LENGTH, ENCODING_LENGTH, DATASET_LENGTH, FILE_TYPE_LENGTH, DATE_CREATED_LENGTH,
                DATE_MODIFIED_LENGTH, HEADER_SIZE_LENGTH, PAGE_SIZE_LENGTH, PAGE_COUNT_LENGTH + align2,
                SAS_RELEASE_LENGTH, SAS_SERVER_TYPE_LENGTH, OS_VERSION_NUMBER_LENGTH, OS_MAKER_LENGTH, OS_NAME_LENGTH,
        };
        List<byte[]> vars = getBytesFromFile(offset, length);

        sasFileProperties.setEndianness(vars.get(0)[0]);
        if (!isSasFileValid()) {
            throw new IOException(FILE_NOT_VALID);
        }

        String fileEncoding = SAS_CHARACTER_ENCODINGS.get(vars.get(1)[0]);
        if (builderEncoding != null) {
            this.encoding = builderEncoding;
        }
        else {
            this.encoding = fileEncoding != null ? fileEncoding : this.encoding;
        }
        sasFileProperties.setEncoding(fileEncoding);
        sasFileProperties.setName(bytesToString(vars.get(2)).trim());
        sasFileProperties.setFileType(bytesToString(vars.get(3)).trim());
        sasFileProperties.setDateCreated(bytesToDateTime(vars.get(4)));
        sasFileProperties.setDateModified(bytesToDateTime(vars.get(5)));
        sasFileProperties.setHeaderLength(bytesToInt(vars.get(6)));
        int pageLength = bytesToInt(vars.get(7));
        if (pageLength > MAX_PAGE_LENGTH) {
            throw new IOException("Page limit ("
                    + pageLength + ") exceeds maximum: " + MAX_PAGE_LENGTH);
        }
        sasFileProperties.setPageLength(pageLength);
        sasFileProperties.setPageCount(bytesToLong(vars.get(8)));
        sasFileProperties.setSasRelease(bytesToString(vars.get(9)).trim());
        sasFileProperties.setServerType(bytesToString(vars.get(10)).trim());
        sasFileProperties.setOsType(bytesToString(vars.get(11)).trim());
        if (vars.get(13)[0] != 0) {
            sasFileProperties.setOsName(bytesToString(vars.get(13)).trim());
        }
        else {
            sasFileProperties.setOsName(bytesToString(vars.get(12)).trim());
        }

        if (sasFileStream != null) {
            skipBytes(sasFileProperties.getHeaderLength() - currentFilePosition);
            currentFilePosition = 0;
        }
    }

    public void skipBytes(long numberOfBytesToSkip)
            throws IOException
    {
        long remainBytes = numberOfBytesToSkip;
        long readBytes;
        while (remainBytes > 0) {
            try {
                readBytes = sasFileStream.read(
                        SKIP_BYTE_BUFFER,
                        0,
                        (int) Math.min(remainBytes, SKIP_BYTE_BUFFER.length));
                if (readBytes < 0) { // EOF
                    break;
                }
            }
            catch (IOException e) {
                throw new IOException(EMPTY_INPUT_STREAM);
            }
            remainBytes -= readBytes;
        }

        long actuallySkipped = numberOfBytesToSkip - remainBytes;

        if (actuallySkipped != numberOfBytesToSkip) {
            throw new IOException("Expected to skip " + numberOfBytesToSkip
                    + " to the end of the header, but skipped " + actuallySkipped + " instead.");
        }
    }

    private boolean isSasFileValid()
    {
        return sasFileProperties.getEndianness() == LITTLE_ENDIAN_CHECKER
                || sasFileProperties.getEndianness() == BIG_ENDIAN_CHECKER;
    }

    private boolean processSasFilePageMeta()
            throws IOException
    {
        int bitOffset = sasFileProperties.isU64() ? PAGE_BIT_OFFSET_X64 : PAGE_BIT_OFFSET_X86;
        readPageHeader();
        List<SubheaderPointer> subheaderPointers = new ArrayList<>();
        if (PageType.PAGE_TYPE_META.contains(currentPageType) || PageType.PAGE_TYPE_MIX.contains(currentPageType)) {
            processPageMetadata(bitOffset, subheaderPointers);
        }
        return PageType.PAGE_TYPE_DATA.contains(currentPageType) || PageType.PAGE_TYPE_MIX.contains(currentPageType)
                || currentPageDataSubheaderPointers.size() != 0;
    }

    private void processPageMetadata(int bitOffset, List<SubheaderPointer> subheaderPointers)
            throws IOException
    {
        subheaderPointers.clear();
        for (int subheaderPointerIndex = 0;subheaderPointerIndex < currentPageSubheadersCount;
                subheaderPointerIndex++) {
            try {
                SubheaderPointer currentSubheaderPointer = processSubheaderPointers((long) bitOffset
                        + SUBHEADER_POINTERS_OFFSET, subheaderPointerIndex);
                subheaderPointers.add(currentSubheaderPointer);
                if (currentSubheaderPointer.compression != TRUNCATED_SUBHEADER_ID) {
                    long subheaderSignature = readSubheaderSignature(currentSubheaderPointer.offset);
                    SubheaderIndexes subheaderIndex = chooseSubheaderClass(
                            subheaderSignature,
                            currentSubheaderPointer.compression,
                            currentSubheaderPointer.type);
                    if (subheaderIndex != null) {
                        if (subheaderIndex != SubheaderIndexes.DATA_SUBHEADER_INDEX) {
                            LOGGER.debug(SUBHEADER_PROCESS_FUNCTION_NAME, subheaderIndex);
                            subheaderIndexToClass.get(subheaderIndex).processSubheader(
                                    subheaderPointers.get(subheaderPointerIndex).offset,
                                    subheaderPointers.get(subheaderPointerIndex).length);
                        }
                        else {
                            currentPageDataSubheaderPointers.add(subheaderPointers.get(subheaderPointerIndex));
                        }
                    }
                    else {
                        LOGGER.debug(UNKNOWN_SUBHEADER_SIGNATURE);
                    }
                }
            }
            catch (Exception e) {
                LOGGER.warn("Encountered broken page metadata. Skipping subheader.");
            }
        }
    }

    private long readSubheaderSignature(Long subheaderPointerOffset)
            throws IOException
    {
        int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
        Long[] subheaderOffsetMass = {subheaderPointerOffset};
        Integer[] subheaderLengthMass = {intOrLongLength};
        List<byte[]> subheaderSignatureMass = getBytesFromFile(
                subheaderOffsetMass,
                subheaderLengthMass);
        return bytesToLong(subheaderSignatureMass.get(0));
    }

    private SubheaderIndexes chooseSubheaderClass(long subheaderSignature, int compression, int type)
    {
        SubheaderIndexes subheaderIndex = SUBHEADER_SIGNATURE_TO_INDEX.get(subheaderSignature);
        if (sasFileProperties.isCompressed() && subheaderIndex == null && (compression == COMPRESSED_SUBHEADER_ID
                || compression == 0) && type == COMPRESSED_SUBHEADER_TYPE) {
            subheaderIndex = SubheaderIndexes.DATA_SUBHEADER_INDEX;
        }
        return subheaderIndex;
    }

    private SubheaderPointer processSubheaderPointers(long subheaderPointerOffset, int subheaderPointerIndex)
            throws IOException
    {
        int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
        int subheaderPointerLength = sasFileProperties.isU64() ? SUBHEADER_POINTER_LENGTH_X64
                : SUBHEADER_POINTER_LENGTH_X86;
        long totalOffset = subheaderPointerOffset + subheaderPointerLength * ((long) subheaderPointerIndex);
        Long[] offset = {
                totalOffset, totalOffset + intOrLongLength, totalOffset + 2L * intOrLongLength,
                totalOffset + 2L * intOrLongLength + 1,
        };
        Integer[] length = {intOrLongLength, intOrLongLength, 1, 1};
        List<byte[]> vars = getBytesFromFile(offset, length);

        long subheaderOffset = bytesToLong(vars.get(0));
        long subheaderLength = bytesToLong(vars.get(1));
        byte subheaderCompression = vars.get(2)[0];
        byte subheaderType = vars.get(3)[0];

        return new SubheaderPointer(subheaderOffset, subheaderLength, subheaderCompression, subheaderType);
    }

    private boolean matchCompressionMethod(String compressionMethod)
    {
        if (compressionMethod == null) {
            LOGGER.warn(NULL_COMPRESSION_LITERAL);
            return false;
        }
        if (LITERALS_TO_DECOMPRESSOR.containsKey(compressionMethod)) {
            return true;
        }
        LOGGER.debug(NO_SUPPORTED_COMPRESSION_LITERAL);
        return false;
    }

    Integer getOffset()
    {
        return currentRowInFileIndex;
    }

    public Object[] readNext()
            throws IOException
    {
        return readNext(null, null);
    }

    public Object[] readNext(List<String> columnNames, Map<String, Integer> mapColumns)
            throws IOException
    {
        if (currentRowInFileIndex++ >= sasFileProperties.getRowCount() || eof) {
            return null;
        }
        int bitOffset = sasFileProperties.isU64() ? PAGE_BIT_OFFSET_X64 : PAGE_BIT_OFFSET_X86;
        currentRow = null;
        switch (currentPageType) {
            case PAGE_META_TYPE_1:
            case PAGE_META_TYPE_2:
            case PAGE_CMETA_TYPE:
                if (currentPageDataSubheaderPointers.size() == 0 && currentPageType == PAGE_CMETA_TYPE) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }

                while (!eof && currentPageDataSubheaderPointers.isEmpty()) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                // TODO (parso fork): added for split-range reads — the page range (or the file) can end
                // before a page with data subheaders is found; upstream never reaches this state
                if (currentPageDataSubheaderPointers.isEmpty()) {
                    return null;
                }
                SubheaderPointer currentSubheaderPointer =
                        currentPageDataSubheaderPointers.get(currentRowOnPageIndex++);
                ((ProcessingDataSubheader) subheaderIndexToClass.get(SubheaderIndexes.DATA_SUBHEADER_INDEX))
                        .processSubheader(currentSubheaderPointer.offset, currentSubheaderPointer.length, columnNames, mapColumns);
                if (currentRowOnPageIndex == currentPageDataSubheaderPointers.size()) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                break;
            case PAGE_MIX_TYPE_1:
                // Mix pages that contain all valid records
                int subheaderPointerLength = sasFileProperties.isU64() ? SUBHEADER_POINTER_LENGTH_X64
                        : SUBHEADER_POINTER_LENGTH_X86;
                int alignCorrection = (bitOffset + SUBHEADER_POINTERS_OFFSET + currentPageSubheadersCount
                        * subheaderPointerLength) % BITS_IN_BYTE;

                currentRow = processByteArrayWithData(bitOffset + SUBHEADER_POINTERS_OFFSET + alignCorrection
                        + currentPageSubheadersCount * subheaderPointerLength + currentRowOnPageIndex++
                        * sasFileProperties.getRowLength(), sasFileProperties.getRowLength(), columnNames, mapColumns);

                if (currentRowOnPageIndex == Math.min(
                        sasFileProperties.getRowCount(),
                        sasFileProperties.getMixPageRowCount())) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                break;
            case PAGE_MIX_TYPE_2:
                // Mix pages that contain valid and deleted records
                if (java.util.Objects.equals(deletedMarkers, "")) {
                    readDeletedInfo();
                    LOGGER.info(deletedMarkers);
                }
                subheaderPointerLength = sasFileProperties.isU64() ? SUBHEADER_POINTER_LENGTH_X64
                        : SUBHEADER_POINTER_LENGTH_X86;
                alignCorrection = (bitOffset + SUBHEADER_POINTERS_OFFSET + currentPageSubheadersCount
                        * subheaderPointerLength) % BITS_IN_BYTE;

                if (deletedMarkers.charAt(currentRowOnPageIndex) == '0') {
                    currentRow = processByteArrayWithData(bitOffset + SUBHEADER_POINTERS_OFFSET + alignCorrection
                            + currentPageSubheadersCount * subheaderPointerLength + currentRowOnPageIndex++
                            * sasFileProperties.getRowLength(), sasFileProperties.getRowLength(), columnNames, mapColumns);
                }
                else {
                    currentRowOnPageIndex++;
                }
                if (currentRowOnPageIndex == Math.min(
                        sasFileProperties.getRowCount(),
                        sasFileProperties.getMixPageRowCount())) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                break;
            case PAGE_DATA_TYPE:
                // Data pages that contain all valid records
                currentRow = processByteArrayWithData(bitOffset + SUBHEADER_POINTERS_OFFSET + currentRowOnPageIndex++
                        * sasFileProperties.getRowLength(), sasFileProperties.getRowLength(), columnNames, mapColumns);
                if (currentRowOnPageIndex == currentPageBlockCount) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                break;
            case PAGE_DATA_TYPE_2:
                // Data pages that contain valid and deleted records
                if (java.util.Objects.equals(deletedMarkers, "")) {
                    readDeletedInfo();
                    LOGGER.info(deletedMarkers);
                    LOGGER.info(Integer.toString(deletedMarkers.length()));
                    LOGGER.info(Integer.toString(currentPageBlockCount));
                }
                if (deletedMarkers.charAt(currentRowOnPageIndex) == '0') {
                    currentRow = processByteArrayWithData(bitOffset + SUBHEADER_POINTERS_OFFSET
                            + currentRowOnPageIndex++
                            * sasFileProperties.getRowLength(), sasFileProperties.getRowLength(), columnNames, mapColumns);
                }
                else {
                    currentRowOnPageIndex++;
                }
                if (currentRowOnPageIndex == currentPageBlockCount) {
                    readNextPage(false);
                    currentRowOnPageIndex = 0;
                }
                break;
            default:
                break;
        }
        if (currentRow == null) {
            return null;
        }
        return java.util.Arrays.copyOf(currentRow, currentRow.length);
    }

    public void readNextPage(boolean skip)
            throws IOException
    {
        deletedMarkers = "";
        processNextPage(skip);
        while (!PageType.PAGE_TYPE_META.contains(currentPageType) && !PageType.PAGE_TYPE_MIX.contains(currentPageType)
                && !PageType.PAGE_TYPE_DATA.contains(currentPageType)) {
            if (eof) {
                return;
            }
            processNextPage(skip);
        }
    }

    private void processNextPage(boolean skip)
            throws IOException
    {
        int bitOffset = sasFileProperties.isU64() ? PAGE_BIT_OFFSET_X64 : PAGE_BIT_OFFSET_X86;
        currentPageDataSubheaderPointers.clear();

        // TODO (parso fork): added for split-range reads — stop before consuming a page outside this reader's range
        if (maxPageExclusive >= 0 && currentPageIndex + 1 >= maxPageExclusive) {
            eof = true;
            return;
        }

        try {
            if (skip) {
                // TODO (parso fork): skipNBytes instead of skip — InputStream.skip may consume fewer
                // bytes than requested, which would desynchronize currentPageIndex from the stream
                sasFileStream.skipNBytes(sasFileProperties.getPageLength());
            }
            else {
                sasFileStream.readFully(cachedPage, 0, sasFileProperties.getPageLength());
            }
        }
        catch (EOFException ex) {
            eof = true;
            return;
        }
        // TODO (parso fork): track the stream index of the loaded page for split-range reads
        currentPageIndex++;

        if (!skip) {
            readPageHeader();
            if (PageType.PAGE_TYPE_META.contains(currentPageType) || PageType.PAGE_TYPE_AMD.contains(currentPageType)
                    || PageType.PAGE_TYPE_MIX.contains(currentPageType)) {
                List<SubheaderPointer> subheaderPointers = new ArrayList<>();
                processPageMetadata(bitOffset, subheaderPointers);
                if (!skip) {
                    readDeletedInfo();
                    if (PageType.PAGE_TYPE_AMD.contains(currentPageType)) {
                        processMissingColumnInfo();
                    }
                }
            }
        }
    }

    private void processMissingColumnInfo()
            throws UnsupportedEncodingException
    {
        for (ColumnMissingInfo columnMissingInfo : columnMissingInfoList) {
            String missedInfo = bytesToString(
                    columnsNamesBytes.get(columnMissingInfo.getTextSubheaderIndex()),
                    columnMissingInfo.getOffset(),
                    columnMissingInfo.getLength()).intern();
            Column column = columns.get(columnMissingInfo.getColumnId());
            switch (columnMissingInfo.getMissingInfoType()) {
                case NAME:
                    column.setName(missedInfo);
                    break;
                case FORMAT:
                    column.setFormat(new ColumnFormat(missedInfo));
                    break;
                case LABEL:
                    column.setLabel(missedInfo);
                    break;
                default:
                    break;
            }
        }
    }

    private void readPageHeader()
            throws IOException
    {
        int bitOffset = sasFileProperties.isU64() ? PAGE_BIT_OFFSET_X64 : PAGE_BIT_OFFSET_X86;
        Long[] offset = {
                bitOffset + PAGE_TYPE_OFFSET, bitOffset + BLOCK_COUNT_OFFSET, bitOffset
                        + SUBHEADER_COUNT_OFFSET,
        };
        Integer[] length = {PAGE_TYPE_LENGTH, BLOCK_COUNT_LENGTH, SUBHEADER_COUNT_LENGTH};
        List<byte[]> vars = getBytesFromFile(offset, length);

        currentPageType = bytesToShort(vars.get(0));
        LOGGER.debug(PAGE_TYPE, currentPageType);
        currentPageBlockCount = bytesToShort(vars.get(1));
        LOGGER.debug(BLOCK_COUNT, currentPageBlockCount);
        currentPageSubheadersCount = bytesToShort(vars.get(2));
        LOGGER.debug(SUBHEADER_COUNT, currentPageSubheadersCount);
    }

    private void readDeletedInfo()
            throws IOException
    {
        long deletedPointerOffset;
        int subheaderPointerLength;
        int bitOffset;
        if (sasFileProperties.isU64()) {
            deletedPointerOffset = PAGE_DELETED_POINTER_OFFSET_X64;
            subheaderPointerLength = SUBHEADER_POINTER_LENGTH_X64;
            bitOffset = PAGE_BIT_OFFSET_X64 + 8;
        }
        else {
            deletedPointerOffset = PAGE_DELETED_POINTER_OFFSET_X86;
            subheaderPointerLength = SUBHEADER_POINTER_LENGTH_X86;
            bitOffset = PAGE_BIT_OFFSET_X86 + 8;
        }
        int alignCorrection = (bitOffset + SUBHEADER_POINTERS_OFFSET + currentPageSubheadersCount
                * subheaderPointerLength) % BITS_IN_BYTE;
        List<byte[]> vars = getBytesFromFile(
                new Long[] {deletedPointerOffset},
                new Integer[] {PAGE_DELETED_POINTER_LENGTH});

        long currentPageDeletedPointer = bytesToInt(vars.get(0));
        long deletedMapOffset = bitOffset + currentPageDeletedPointer + alignCorrection
                + (currentPageSubheadersCount * subheaderPointerLength)
                + ((currentPageBlockCount - currentPageSubheadersCount) * sasFileProperties.getRowLength());
        List<byte[]> bytes = getBytesFromFile(
                new Long[] {deletedMapOffset},
                new Integer[] {(int) Math.ceil((currentPageBlockCount - currentPageSubheadersCount) / 8.0)});

        byte[] x = bytes.get(0);
        for (byte b : x) {
            deletedMarkers += String.format("%8s", Integer.toString(b & 0xFF, 2)).replace(" ", "0");
        }
    }

    private Object[] processByteArrayWithData(long rowOffset, long rowLength, List<String> columnNames, Map<String, Integer> mapColumns)
    {
        Object[] rowElements;
        if (columnNames != null) {
            rowElements = new Object[columnNames.size()];
        }
        else {
            rowElements = new Object[(int) sasFileProperties.getColumnsCount()];
        }
        byte[] source;
        int offset;
        if (sasFileProperties.isCompressed() && rowLength < sasFileProperties.getRowLength()) {
            Decompressor decompressor = LITERALS_TO_DECOMPRESSOR.get(sasFileProperties.getCompressionMethod());
            source = decompressor.decompressRow(
                    (int) rowOffset,
                    (int) rowLength,
                    (int) sasFileProperties.getRowLength(),
                    cachedPage);
            offset = 0;
        }
        else {
            source = cachedPage;
            offset = (int) rowOffset;
        }

        for (int currentColumnIndex = 0;currentColumnIndex < sasFileProperties.getColumnsCount()
                && columnsDataLength.get(currentColumnIndex) != 0;currentColumnIndex++) {
            if (columnNames == null) {
                rowElements[currentColumnIndex] = processElement(source, offset, currentColumnIndex);
            }
            else {
                String name = columns.get(currentColumnIndex).getName();
                if (mapColumns.containsKey(name)) {
                    rowElements[mapColumns.get(name)] = processElement(source, offset, currentColumnIndex);
                }
/*                if (columnNames.contains(name))
                {
                    rowElements[columnNames.indexOf(name)] = processElement(source, offset, currentColumnIndex);
                }*/
            }
        }

        return rowElements;
    }

    private Object processElement(byte[] source, int offset, int currentColumnIndex)
    {
        byte[] temp;
        int length = columnsDataLength.get(currentColumnIndex);
        if (columns.get(currentColumnIndex).getType() == Number.class) {
            temp = java.util.Arrays.copyOfRange(
                    source,
                    offset + (int) (long) columnsDataOffset.get(currentColumnIndex),
                    offset + (int) (long) columnsDataOffset.get(currentColumnIndex) + length);
            if (columnsDataLength.get(currentColumnIndex) <= 2) {
                return bytesToShort(temp);
            }
            else {
                if (columns.get(currentColumnIndex).getFormat().getName().isEmpty()) {
                    return convertByteArrayToNumber(temp);
                }
                else {
                    ColumnFormat columnFormat = columns.get(currentColumnIndex).getFormat();
                    String sasDateFormat = columnFormat.getName();
                    if (SasTemporalFormatter.isDateTimeFormat(sasDateFormat)) {
                        return bytesToDateTime(temp, outputDateType, columnFormat);
                    }
                    else if (SasTemporalFormatter.isDateFormat(sasDateFormat)) {
                        return bytesToDate(temp, outputDateType, columnFormat);
                    }
                    else if (SasTemporalFormatter.isTimeFormat(sasDateFormat)) {
                        return bytesToTime(temp, outputDateType, columnFormat);
                    }
                    else {
                        return convertByteArrayToNumber(temp);
                    }
                }
            }
        }
        else {
            byte[] bytes = trimBytesArray(
                    source,
                    offset + columnsDataOffset.get(currentColumnIndex).intValue(),
                    length);
            if (byteOutput) {
                return bytes;
            }
            else {
                try {
                    return (bytes == null ? null : bytesToString(bytes));
                }
                catch (UnsupportedEncodingException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
        return null;
    }

    private List<byte[]> getBytesFromFile(Long[] offset, Integer[] length)
            throws IOException
    {
        List<byte[]> vars = new ArrayList<>();
        if (cachedPage == null) {
            for (int i = 0; i < offset.length; i++) {
                byte[] temp = new byte[length[i]];
                skipBytes(offset[i] - currentFilePosition);
                try {
                    sasFileStream.readFully(temp, 0, length[i]);
                }
                catch (EOFException e) {
                    eof = true;
                }
                currentFilePosition = (int) (long) offset[i] + length[i];
                vars.add(temp);
            }
        }
        else {
            for (int i = 0; i < offset.length; i++) {
                if (cachedPage.length < offset[i]) {
                    throw new IOException(EMPTY_INPUT_STREAM);
                }
                vars.add(java.util.Arrays.copyOfRange(cachedPage, (int) (long) offset[i], (int) (long) offset[i] + length[i]));
            }
        }
        return vars;
    }

    private long correctLongProcess(ByteBuffer byteBuffer)
    {
        if (sasFileProperties.isU64()) {
            return byteBuffer.getLong();
        }
        else {
            return byteBuffer.getInt();
        }
    }

    private ByteBuffer byteArrayToByteBuffer(byte[] data)
    {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        if (sasFileProperties.getEndianness() == 0) {
            return byteBuffer;
        }
        else {
            return byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    private Object convertByteArrayToNumber(byte[] mass)
    {
        double resultDouble = bytesToDouble(mass);

        if (Double.isNaN(resultDouble) || (resultDouble < NAN_EPSILON && resultDouble > 0)) {
            return null;
        }

        long resultLong = Math.round(resultDouble);
        if (Math.abs(resultDouble - resultLong) >= EPSILON) {
            return resultDouble;
        }
        else {
            return resultLong;
        }
    }

    private int bytesToShort(byte[] bytes)
    {
        return byteArrayToByteBuffer(bytes).getShort();
    }

    private int bytesToInt(byte[] bytes)
    {
        return byteArrayToByteBuffer(bytes).getInt();
    }

    private long bytesToLong(byte[] bytes)
    {
        return correctLongProcess(byteArrayToByteBuffer(bytes));
    }

    private String bytesToString(byte[] bytes)
            throws UnsupportedEncodingException
    {
        return new String(bytes, Charset.forName(encoding));
    }

    private String bytesToString(byte[] bytes, int offset, int length)
            throws UnsupportedEncodingException, StringIndexOutOfBoundsException
    {
        return new String(bytes, offset, length, Charset.forName(encoding));
    }

    private Date bytesToDateTime(byte[] bytes)
    {
        double doubleSeconds = bytesToDouble(bytes);
        if (Double.isNaN(doubleSeconds)) {
            return null;
        }
        else {
            return sasTemporalFormatter.formatSasSecondsAsJavaDate(doubleSeconds);
        }
    }

    private Object bytesToDateTime(byte[] bytes, OutputDateType outputDateType, ColumnFormat columnFormat)
    {
        double doubleSeconds = bytesToDouble(bytes);
        return sasTemporalFormatter.formatSasDateTime(
                doubleSeconds,
                outputDateType,
                columnFormat.getName(),
                columnFormat.getWidth(),
                columnFormat.getPrecision());
    }

    private Object bytesToTime(byte[] bytes, OutputDateType outputDateType, ColumnFormat columnFormat)
    {
        double doubleSeconds = bytesToDouble(bytes);
        return sasTemporalFormatter.formatSasTime(
                doubleSeconds,
                outputDateType,
                columnFormat.getName(),
                columnFormat.getWidth(),
                columnFormat.getPrecision());
    }

    private Object bytesToDate(byte[] bytes, OutputDateType outputDateType, ColumnFormat columnFormat)
    {
        double doubleDays = bytesToDouble(bytes);
        return sasTemporalFormatter.formatSasDate(
                doubleDays,
                outputDateType,
                columnFormat.getName(),
                columnFormat.getWidth(),
                columnFormat.getPrecision());
    }

    private double bytesToDouble(byte[] bytes)
    {
        ByteBuffer original = byteArrayToByteBuffer(bytes);

        if (bytes.length < BYTES_IN_DOUBLE) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(BYTES_IN_DOUBLE);
            if (sasFileProperties.getEndianness() == 1) {
                byteBuffer.position(BYTES_IN_DOUBLE - bytes.length);
            }
            byteBuffer.put(original);
            byteBuffer.order(original.order());
            byteBuffer.position(0);
            original = byteBuffer;
        }

        return original.getDouble();
    }

    public static double byte2Double(byte[] inData)
    {
        // convert bytes to int
        long longValue = 0;
        for (byte b : inData) {
            longValue = (longValue << 8) + (b & 0xFF);
        }

        // convert int to float
        double value = Double.longBitsToDouble(longValue);
        return value;
    }

    // TODO (parso fork): method added for split-range reads — not present in upstream SasFileParser.
    // Restricts this reader to rows residing on stream pages [startPage, endPageExclusive).
    // Must be called once, right after construction, before any readNext call. Construction parses
    // metadata pages up to and including the first data-bearing page (currentPageIndex); that page's
    // rows are served only by the reader whose range contains it, so concurrent readers with disjoint
    // ranges cover every row exactly once.
    public void setPageRange(long startPage, long endPageExclusive)
            throws IOException
    {
        maxPageExclusive = endPageExclusive;
        if (currentPageIndex >= endPageExclusive) {
            // metadata parsing already consumed the whole range; only the last metadata page can
            // hold rows and it lies at or past the end of the range, so there is nothing to serve
            currentPageDataSubheaderPointers.clear();
            eof = true;
            return;
        }
        if (currentPageIndex < startPage) {
            // position on the first page of the range, skipping pages in between
            for (long toSkip = startPage - currentPageIndex - 1; toSkip > 0 && !eof; toSkip--) {
                readNextPage(true);
            }
            if (!eof) {
                readNextPage(false);
            }
        }
        // otherwise the page loaded during metadata parsing is inside the range: serve it as-is
    }

    private byte[] trimBytesArray(byte[] source, int offset, int length)
    {
        int lengthFromBegin;
        for (lengthFromBegin = offset + length; lengthFromBegin > offset; lengthFromBegin--) {
            if (source[lengthFromBegin - 1] != ' ' && source[lengthFromBegin - 1] != '\0'
                    && source[lengthFromBegin - 1] != '\t') {
                break;
            }
        }

        if (lengthFromBegin - offset != 0) {
            return java.util.Arrays.copyOfRange(source, offset, lengthFromBegin);
        }
        else {
            return null;
        }
    }

    List<Column> getColumns()
    {
        return columns;
    }

    public SasFileProperties getSasFileProperties()
    {
        return sasFileProperties;
    }

    private enum SubheaderIndexes
    {
        ROW_SIZE_SUBHEADER_INDEX,
        COLUMN_SIZE_SUBHEADER_INDEX,
        SUBHEADER_COUNTS_SUBHEADER_INDEX,
        COLUMN_TEXT_SUBHEADER_INDEX,
        COLUMN_NAME_SUBHEADER_INDEX,
        COLUMN_ATTRIBUTES_SUBHEADER_INDEX,
        FORMAT_AND_LABEL_SUBHEADER_INDEX,
        COLUMN_LIST_SUBHEADER_INDEX,
        DATA_SUBHEADER_INDEX,
    }

    private interface ProcessingSubheader
    {
        void processSubheader(long subheaderOffset, long subheaderLength) throws IOException;
    }

    private interface ProcessingDataSubheader
            extends ProcessingSubheader
    {
        void processSubheader(long subheaderOffset, long subheaderLength, List<String> columnNames, Map<String, Integer> mapColumns) throws IOException;
    }

    public static class Builder
    {
        private Builder() {}

        private InputStream sasFileStream;

        private String encoding;

        private OutputDateType outputDateType = OutputDateType.JAVA_DATE_LEGACY;

        private Boolean byteOutput = false;

        public Builder(InputStream sasFileStream)
        {
            this.sasFileStream = sasFileStream;
        }

        public Builder encoding(String val)
        {
            encoding = val;
            return this;
        }

        public Builder outputDateType(OutputDateType val)
        {
            if (val != null) {
                outputDateType = val;
            }
            return this;
        }

        public Builder byteOutput(Boolean val)
        {
            byteOutput = val;
            return this;
        }

        public CustomSasFileParser build()
                throws IOException
        {
            return new CustomSasFileParser(this);
        }
    }

    static class SubheaderPointer
    {
        private final long offset;

        private final long length;

        private final byte compression;

        private final byte type;

        SubheaderPointer(long offset, long length, byte compression, byte type)
        {
            this.offset = offset;
            this.length = length;
            this.compression = compression;
            this.type = type;
        }
    }

    class RowSizeSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            Long[] offset = {
                    subheaderOffset + ROW_LENGTH_OFFSET_MULTIPLIER * intOrLongLength,
                    subheaderOffset + ROW_COUNT_OFFSET_MULTIPLIER * intOrLongLength,
                    subheaderOffset + ROW_COUNT_ON_MIX_PAGE_OFFSET_MULTIPLIER * intOrLongLength,
                    subheaderOffset + FILE_FORMAT_OFFSET_OFFSET + 82 * intOrLongLength,
                    subheaderOffset + FILE_FORMAT_LENGTH_OFFSET + 82 * intOrLongLength,
                    subheaderOffset + DELETED_ROW_COUNT_OFFSET_MULTIPLIER * intOrLongLength,
                    subheaderOffset + COMPRESSION_METHOD_OFFSET + 82 * intOrLongLength,
                    subheaderOffset + COMPRESSION_METHOD_LENGTH_OFFSET + 82 * intOrLongLength,
            };
            Integer[] length = {
                    intOrLongLength, intOrLongLength, intOrLongLength,
                    FILE_FORMAT_OFFSET_LENGTH, FILE_FORMAT_LENGTH_LENGTH,
                    intOrLongLength,
                    COMPRESSION_METHOD_OFFSET_LENGTH, COMPRESSION_METHOD_LENGTH_LENGTH,
            };
            List<byte[]> vars = getBytesFromFile(offset, length);

            if (sasFileProperties.getRowLength() == 0) {
                sasFileProperties.setRowLength(bytesToLong(vars.get(0)));
            }
            if (sasFileProperties.getRowCount() == 0) {
                sasFileProperties.setRowCount(bytesToLong(vars.get(1)));
            }
            if (sasFileProperties.getMixPageRowCount() == 0) {
                sasFileProperties.setMixPageRowCount(bytesToLong(vars.get(2)));
            }

            fileLabelOffset = bytesToShort(vars.get(3));
            fileLabelLength = bytesToShort(vars.get(4));

            if (sasFileProperties.getDeletedRowCount() == 0) {
                sasFileProperties.setDeletedRowCount(bytesToLong(vars.get(5)));
            }

            compressionMethodOffset = bytesToShort(vars.get(6));
            compressionMethodLength = bytesToShort(vars.get(7));
        }
    }

    class ColumnSizeSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            Long[] offset = {subheaderOffset + intOrLongLength};
            Integer[] length = {intOrLongLength};
            List<byte[]> vars = getBytesFromFile(offset, length);

            sasFileProperties.setColumnsCount(bytesToLong(vars.get(0)));
        }
    }

    static class SubheaderCountsSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {}
    }

    class ColumnTextSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            int textBlockSize;

            Long[] offset = {subheaderOffset + intOrLongLength};
            Integer[] length = {TEXT_BLOCK_SIZE_LENGTH};
            List<byte[]> vars = getBytesFromFile(offset, length);
            textBlockSize = byteArrayToByteBuffer(vars.get(0)).getShort();

            offset[0] = subheaderOffset + intOrLongLength;
            length[0] = textBlockSize;
            vars = getBytesFromFile(offset, length);

            columnsNamesBytes.add(vars.get(0));
            if (columnsNamesBytes.size() == 1) {
                byte[] columnName = columnsNamesBytes.get(0);
                String compressionMethod = bytesToString(columnName, compressionMethodOffset, compressionMethodLength);
                if (matchCompressionMethod(compressionMethod)) {
                    sasFileProperties.setCompressionMethod(compressionMethod);
                }
                sasFileProperties.setFileLabel(bytesToString(columnName, fileLabelOffset, fileLabelLength));
            }
        }
    }

    class ColumnNameSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            long columnNamePointersCount = (subheaderLength - 2 * intOrLongLength - 12) / 8;
            int i;
            for (i = 0; i < columnNamePointersCount; i++) {
                Long[] offset = {
                        subheaderOffset + intOrLongLength + COLUMN_NAME_POINTER_LENGTH * (i + 1)
                                + COLUMN_NAME_TEXT_SUBHEADER_OFFSET, subheaderOffset + intOrLongLength
                                + COLUMN_NAME_POINTER_LENGTH * (i + 1) + COLUMN_NAME_OFFSET_OFFSET, subheaderOffset
                                + intOrLongLength + COLUMN_NAME_POINTER_LENGTH * (i + 1) + COLUMN_NAME_LENGTH_OFFSET,
                };
                Integer[] length = {
                        COLUMN_NAME_TEXT_SUBHEADER_LENGTH, COLUMN_NAME_OFFSET_LENGTH,
                        COLUMN_NAME_LENGTH_LENGTH,
                };
                List<byte[]> vars = getBytesFromFile(offset, length);

                int textSubheaderIndex = bytesToShort(vars.get(0));
                int columnNameOffset = bytesToShort(vars.get(1));
                int columnNameLength = bytesToShort(vars.get(2));
                if (textSubheaderIndex < columnsNamesBytes.size()) {
                    columnsNamesList.add(bytesToString(
                            columnsNamesBytes.get(textSubheaderIndex),
                            columnNameOffset,
                            columnNameLength).intern());
                }
                else {
                    columnsNamesList.add(new String(new char[columnNameLength]));
                    columnMissingInfoList.add(new ColumnMissingInfo(
                            i,
                            textSubheaderIndex,
                            columnNameOffset,
                            columnNameLength,
                            ColumnMissingInfo.MissingInfoType.NAME));
                }
            }
        }
    }

    class ColumnAttributesSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            long columnAttributesVectorsCount = (subheaderLength - 2 * intOrLongLength - 12) / (intOrLongLength + 8);
            for (int i = 0; i < columnAttributesVectorsCount; i++) {
                Long[] offset = {
                        subheaderOffset + intOrLongLength + COLUMN_DATA_OFFSET_OFFSET + i
                                * (intOrLongLength + 8), subheaderOffset + 2 * intOrLongLength + COLUMN_DATA_LENGTH_OFFSET + i
                                * (intOrLongLength + 8), subheaderOffset + 2 * intOrLongLength + COLUMN_TYPE_OFFSET + i
                                * (intOrLongLength + 8),
                };
                Integer[] length = {intOrLongLength, COLUMN_DATA_LENGTH_LENGTH, COLUMN_TYPE_LENGTH};
                List<byte[]> vars = getBytesFromFile(offset, length);

                columnsDataOffset.add(bytesToLong(vars.get(0)));
                columnsDataLength.add(bytesToInt(vars.get(1)));
                columnsTypesList.add(vars.get(2)[0] == 1 ? Number.class : String.class);
            }
        }
    }

    class FormatAndLabelSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            int intOrLongLength = sasFileProperties.isU64() ? BYTES_IN_LONG : BYTES_IN_INT;
            Long[] offset = {
                    subheaderOffset + COLUMN_FORMAT_WIDTH_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_FORMAT_PRECISION_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_FORMAT_OFFSET_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_FORMAT_LENGTH_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_LABEL_TEXT_SUBHEADER_INDEX_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_LABEL_OFFSET_OFFSET + 3 * intOrLongLength,
                    subheaderOffset + COLUMN_LABEL_LENGTH_OFFSET + 3 * intOrLongLength,
            };
            Integer[] length = {
                    COLUMN_FORMAT_WIDTH_OFFSET_LENGTH, COLUMN_FORMAT_PRECISION_OFFSET_LENGTH,
                    COLUMN_FORMAT_TEXT_SUBHEADER_INDEX_LENGTH, COLUMN_FORMAT_OFFSET_LENGTH, COLUMN_FORMAT_LENGTH_LENGTH,
                    COLUMN_LABEL_TEXT_SUBHEADER_INDEX_LENGTH, COLUMN_LABEL_OFFSET_LENGTH, COLUMN_LABEL_LENGTH_LENGTH,
            };
            List<byte[]> vars = getBytesFromFile(offset, length);

            int columnFormatWidth = bytesToShort(vars.get(0));
            int columnFormatPrecision = bytesToShort(vars.get(1));
            int textSubheaderIndexForFormat = bytesToShort(vars.get(2));
            int columnFormatOffset = bytesToShort(vars.get(3));
            int columnFormatLength = bytesToShort(vars.get(4));
            int textSubheaderIndexForLabel = bytesToShort(vars.get(5));
            int columnLabelOffset = bytesToShort(vars.get(6));
            int columnLabelLength = bytesToShort(vars.get(7));
            String columnLabel = "";
            String columnFormatName = "";
            if (textSubheaderIndexForLabel < columnsNamesBytes.size()) {
                columnLabel = bytesToString(
                        columnsNamesBytes.get(textSubheaderIndexForLabel),
                        columnLabelOffset,
                        columnLabelLength).intern();
            }
            else {
                columnMissingInfoList.add(new ColumnMissingInfo(
                        columns.size(),
                        textSubheaderIndexForLabel,
                        columnLabelOffset,
                        columnLabelLength,
                        ColumnMissingInfo.MissingInfoType.LABEL));
            }
            if (textSubheaderIndexForFormat < columnsNamesBytes.size()) {
                columnFormatName = bytesToString(
                        columnsNamesBytes.get(textSubheaderIndexForFormat),
                        columnFormatOffset,
                        columnFormatLength).intern();
            }
            else {
                columnMissingInfoList.add(new ColumnMissingInfo(
                        columns.size(),
                        textSubheaderIndexForFormat,
                        columnFormatOffset,
                        columnFormatLength,
                        ColumnMissingInfo.MissingInfoType.FORMAT));
            }
            LOGGER.debug(COLUMN_FORMAT, columnFormatName);
            ColumnFormat columnFormat = new ColumnFormat(columnFormatName, columnFormatWidth, columnFormatPrecision);
            columns.add(new Column(
                    currentColumnNumber + 1,
                    columnsNamesList.get(columns.size()),
                    columnLabel,
                    columnFormat,
                    columnsTypesList.get(columns.size()),
                    columnsDataLength.get(currentColumnNumber++)));
        }
    }

    static class ColumnListSubheader
            implements ProcessingSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {}
    }

    class DataSubheader
            implements ProcessingDataSubheader
    {
        @Override
        public void processSubheader(long subheaderOffset, long subheaderLength)
                throws IOException
        {
            currentRow = processByteArrayWithData(subheaderOffset, subheaderLength, null, null);
        }

        @Override
        public void processSubheader(
                long subheaderOffset,
                long subheaderLength,
                List<String> columnNames,
                Map<String, Integer> mapColumns)
                throws IOException
        {
            currentRow = processByteArrayWithData(subheaderOffset, subheaderLength, columnNames, mapColumns);
        }
    }
}
