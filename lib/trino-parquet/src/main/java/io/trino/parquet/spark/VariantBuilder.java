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
package io.trino.parquet.spark;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.parquet.spark.VariantUtil.DECIMAL16;
import static io.trino.parquet.spark.VariantUtil.DECIMAL4;
import static io.trino.parquet.spark.VariantUtil.DECIMAL8;
import static io.trino.parquet.spark.VariantUtil.DOUBLE;
import static io.trino.parquet.spark.VariantUtil.FALSE;
import static io.trino.parquet.spark.VariantUtil.INT1;
import static io.trino.parquet.spark.VariantUtil.INT2;
import static io.trino.parquet.spark.VariantUtil.INT4;
import static io.trino.parquet.spark.VariantUtil.INT8;
import static io.trino.parquet.spark.VariantUtil.LONG_STR;
import static io.trino.parquet.spark.VariantUtil.MAX_DECIMAL16_PRECISION;
import static io.trino.parquet.spark.VariantUtil.MAX_DECIMAL4_PRECISION;
import static io.trino.parquet.spark.VariantUtil.MAX_DECIMAL8_PRECISION;
import static io.trino.parquet.spark.VariantUtil.MAX_SHORT_STR_SIZE;
import static io.trino.parquet.spark.VariantUtil.NULL;
import static io.trino.parquet.spark.VariantUtil.SIZE_LIMIT;
import static io.trino.parquet.spark.VariantUtil.TRUE;
import static io.trino.parquet.spark.VariantUtil.U16_MAX;
import static io.trino.parquet.spark.VariantUtil.U24_MAX;
import static io.trino.parquet.spark.VariantUtil.U24_SIZE;
import static io.trino.parquet.spark.VariantUtil.U32_SIZE;
import static io.trino.parquet.spark.VariantUtil.U8_MAX;
import static io.trino.parquet.spark.VariantUtil.VERSION;
import static io.trino.parquet.spark.VariantUtil.arrayHeader;
import static io.trino.parquet.spark.VariantUtil.objectHeader;
import static io.trino.parquet.spark.VariantUtil.primitiveHeader;
import static io.trino.parquet.spark.VariantUtil.shortStrHeader;
import static io.trino.parquet.spark.VariantUtil.writeLong;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;

/**
 * Copied from https://github.com/apache/spark/blob/53d65fd12dd9231139188227ef9040d40d759021/common/variant/src/main/java/org/apache/spark/types/variant/VariantBuilder.java

 * Build variant value and metadata by parsing JSON values.
 */
public class VariantBuilder
{
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder().build();

    /**
     * Parse a JSON string as a Variant value.
     *
     * @throws VariantSizeLimitException if the resulting variant value or metadata would exceed
     *         the SIZE_LIMIT (for example, this could be a maximum of 16 MiB).
     * @throws IOException if any JSON parsing error happens.
     */
    public static Variant parseJson(String json)
            throws IOException
    {
        try (JsonParser parser = JSON_FACTORY.createParser(json)) {
            parser.nextToken();
            return parseJson(parser);
        }
    }

    /**
     * Similar {@link #parseJson(String)}, but takes a JSON parser instead of string input.
     */
    public static Variant parseJson(JsonParser parser)
            throws IOException
    {
        VariantBuilder builder = new VariantBuilder();
        builder.buildJson(parser);
        return builder.result();
    }

    // Build the variant metadata from `dictionaryKeys` and return the variant result.
    public Variant result()
    {
        int numKeys = dictionaryKeys.size();
        // Use long to avoid overflow in accumulating lengths.
        long dictionaryStringSize = 0;
        for (byte[] key : dictionaryKeys) {
            dictionaryStringSize += key.length;
        }
        // Determine the number of bytes required per offset entry.
        // The largest offset is the one-past-the-end value, which is total string size. It's very
        // unlikely that the number of keys could be larger, but incorporate that into the calcualtion
        // in case of pathological data.
        long maxSize = Math.max(dictionaryStringSize, numKeys);
        if (maxSize > SIZE_LIMIT) {
            throw new VariantSizeLimitException();
        }
        int offsetSize = getIntegerSize((int) maxSize);

        int offsetStart = 1 + offsetSize;
        int stringStart = offsetStart + (numKeys + 1) * offsetSize;
        long metadataSize = stringStart + dictionaryStringSize;

        if (metadataSize > SIZE_LIMIT) {
            throw new VariantSizeLimitException();
        }
        byte[] metadata = new byte[(int) metadataSize];
        int headerByte = VERSION | ((offsetSize - 1) << 6);
        writeLong(metadata, 0, headerByte, 1);
        writeLong(metadata, 1, numKeys, offsetSize);
        int currentOffset = 0;
        for (int i = 0; i < numKeys; ++i) {
            writeLong(metadata, offsetStart + i * offsetSize, currentOffset, offsetSize);
            byte[] key = dictionaryKeys.get(i);
            System.arraycopy(key, 0, metadata, stringStart + currentOffset, key.length);
            currentOffset += key.length;
        }
        writeLong(metadata, offsetStart + numKeys * offsetSize, currentOffset, offsetSize);
        return new Variant(Arrays.copyOfRange(writeBuffer, 0, writePos), metadata);
    }

    public void appendString(String str)
    {
        byte[] text = str.getBytes(StandardCharsets.UTF_8);
        boolean longStr = text.length > MAX_SHORT_STR_SIZE;
        checkCapacity((longStr ? 1 + U32_SIZE : 1) + text.length);
        if (longStr) {
            writeBuffer[writePos++] = primitiveHeader(LONG_STR);
            writeLong(writeBuffer, writePos, text.length, U32_SIZE);
            writePos += U32_SIZE;
        }
        else {
            writeBuffer[writePos++] = shortStrHeader(text.length);
        }
        System.arraycopy(text, 0, writeBuffer, writePos, text.length);
        writePos += text.length;
    }

    public void appendNull()
    {
        checkCapacity(1);
        writeBuffer[writePos++] = primitiveHeader(NULL);
    }

    public void appendBoolean(boolean b)
    {
        checkCapacity(1);
        writeBuffer[writePos++] = primitiveHeader(b ? TRUE : FALSE);
    }

    // Append a long value to the variant builder. The actual used integer type depends on the value
    // range of the long value.
    public void appendLong(long l)
    {
        checkCapacity(1 + 8);
        if (l == (byte) l) {
            writeBuffer[writePos++] = primitiveHeader(INT1);
            writeLong(writeBuffer, writePos, l, 1);
            writePos += 1;
        }
        else if (l == (short) l) {
            writeBuffer[writePos++] = primitiveHeader(INT2);
            writeLong(writeBuffer, writePos, l, 2);
            writePos += 2;
        }
        else if (l == (int) l) {
            writeBuffer[writePos++] = primitiveHeader(INT4);
            writeLong(writeBuffer, writePos, l, 4);
            writePos += 4;
        }
        else {
            writeBuffer[writePos++] = primitiveHeader(INT8);
            writeLong(writeBuffer, writePos, l, 8);
            writePos += 8;
        }
    }

    public void appendDouble(double d)
    {
        checkCapacity(1 + 8);
        writeBuffer[writePos++] = primitiveHeader(DOUBLE);
        writeLong(writeBuffer, writePos, Double.doubleToLongBits(d), 8);
        writePos += 8;
    }

    // Append a decimal value to the variant builder. The caller should guarantee that its precision
    // and scale fit into `MAX_DECIMAL16_PRECISION`.
    public void appendDecimal(BigDecimal d)
    {
        checkCapacity(2 + 16);
        BigInteger unscaled = d.unscaledValue();
        if (d.scale() <= MAX_DECIMAL4_PRECISION && d.precision() <= MAX_DECIMAL4_PRECISION) {
            writeBuffer[writePos++] = primitiveHeader(DECIMAL4);
            writeBuffer[writePos++] = (byte) d.scale();
            writeLong(writeBuffer, writePos, unscaled.intValueExact(), 4);
            writePos += 4;
        }
        else if (d.scale() <= MAX_DECIMAL8_PRECISION && d.precision() <= MAX_DECIMAL8_PRECISION) {
            writeBuffer[writePos++] = primitiveHeader(DECIMAL8);
            writeBuffer[writePos++] = (byte) d.scale();
            writeLong(writeBuffer, writePos, unscaled.longValueExact(), 8);
            writePos += 8;
        }
        else {
            checkState(d.scale() <= MAX_DECIMAL16_PRECISION && d.precision() <= MAX_DECIMAL16_PRECISION);
            writeBuffer[writePos++] = primitiveHeader(DECIMAL16);
            writeBuffer[writePos++] = (byte) d.scale();
            // `toByteArray` returns a big-endian representation. We need to copy it reversely and sign
            // extend it to16 bytes.
            byte[] bytes = unscaled.toByteArray();
            for (int i = 0; i < bytes.length; ++i) {
                writeBuffer[writePos + i] = bytes[bytes.length - 1 - i];
            }
            byte sign = (byte) (bytes[0] < 0 ? -1 : 0);
            for (int i = bytes.length; i < 16; ++i) {
                writeBuffer[writePos + i] = sign;
            }
            writePos += 16;
        }
    }

    // Add a key to the variant dictionary. If the key already exists, the dictionary is not modified.
    // In either case, return the id of the key.
    public int addKey(String key)
    {
        int id;
        if (dictionary.containsKey(key)) {
            id = dictionary.get(key);
        }
        else {
            id = dictionaryKeys.size();
            dictionary.put(key, id);
            dictionaryKeys.add(key.getBytes(StandardCharsets.UTF_8));
        }
        return id;
    }

    // Return the current write position of the variant builder. It is used together with
    // `finishWritingObject` or `finishWritingArray`.
    public int getWritePos()
    {
        return writePos;
    }

    // Finish writing a variant object after all of its fields have already been written. The process
    // is as follows:
    // 1. The caller calls `getWritePos` before writing any fields to obtain the `start` parameter.
    // 2. The caller appends all the object fields to the builder. In the meantime, it should maintain
    // the `fields` parameter. Before appending each field, it should append an entry to `fields` to
    // record the offset of the field. The offset is computed as `getWritePos() - start`.
    // 3. The caller calls `finishWritingObject` to finish writing a variant object.
    //
    // This function is responsible to sort the fields by key and check for any duplicate field keys.
    public void finishWritingObject(int start, ArrayList<FieldEntry> fields)
    {
        int dataSize = writePos - start;
        int size = fields.size();
        Collections.sort(fields);
        int maxId = size == 0 ? 0 : fields.get(0).id;
        // Check for duplicate field keys. Only need to check adjacent key because they are sorted.
        for (int i = 1; i < size; ++i) {
            maxId = Math.max(maxId, fields.get(i).id);
            String key = fields.get(i).key;
            if (key.equals(fields.get(i - 1).key)) {
                throw new IllegalArgumentException("Duplicate key in variant object: " + key);
            }
        }
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U32_SIZE : 1;
        int idSize = getIntegerSize(maxId);
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, id list, and offset list.
        int headerSize = 1 + sizeBytes + size * idSize + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the object header section.
        System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
        writePos += headerSize;
        writeBuffer[start] = objectHeader(largeSize, idSize, offsetSize);
        writeLong(writeBuffer, start + 1, size, sizeBytes);
        int idStart = start + 1 + sizeBytes;
        int offsetStart = idStart + size * idSize;
        for (int i = 0; i < size; ++i) {
            writeLong(writeBuffer, idStart + i * idSize, fields.get(i).id, idSize);
            writeLong(writeBuffer, offsetStart + i * offsetSize, fields.get(i).offset, offsetSize);
        }
        writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    }

    // Finish writing a variant array after all of its elements have already been written. The process
    // is similar to that of `finishWritingObject`.
    public void finishWritingArray(int start, ArrayList<Integer> offsets)
    {
        int dataSize = writePos - start;
        int size = offsets.size();
        boolean largeSize = size > U8_MAX;
        int sizeBytes = largeSize ? U32_SIZE : 1;
        int offsetSize = getIntegerSize(dataSize);
        // The space for header byte, object size, and offset list.
        int headerSize = 1 + sizeBytes + (size + 1) * offsetSize;
        checkCapacity(headerSize);
        // Shift the just-written field data to make room for the header section.
        System.arraycopy(writeBuffer, start, writeBuffer, start + headerSize, dataSize);
        writePos += headerSize;
        writeBuffer[start] = arrayHeader(largeSize, offsetSize);
        writeLong(writeBuffer, start + 1, size, sizeBytes);
        int offsetStart = start + 1 + sizeBytes;
        for (int i = 0; i < size; ++i) {
            writeLong(writeBuffer, offsetStart + i * offsetSize, offsets.get(i), offsetSize);
        }
        writeLong(writeBuffer, offsetStart + size * offsetSize, dataSize, offsetSize);
    }

    private void checkCapacity(int additional)
    {
        int required = writePos + additional;
        if (required > writeBuffer.length) {
            // Allocate a new buffer with a capacity of the next power of 2 of `required`.
            int newCapacity = Integer.highestOneBit(required);
            newCapacity = newCapacity < required ? newCapacity * 2 : newCapacity;
            if (newCapacity > SIZE_LIMIT) {
                throw new VariantSizeLimitException();
            }
            byte[] newValue = new byte[newCapacity];
            System.arraycopy(writeBuffer, 0, newValue, 0, writePos);
            writeBuffer = newValue;
        }
    }

    // Temporarily store the information of a field. We need to collect all fields in an JSON object,
    // sort them by their keys, and build the variant object in sorted order.
    public static final class FieldEntry
            implements Comparable<FieldEntry>
    {
        final String key;
        final int id;
        final int offset;

        public FieldEntry(String key, int id, int offset)
        {
            this.key = key;
            this.id = id;
            this.offset = offset;
        }

        @Override
        public int compareTo(FieldEntry other)
        {
            return key.compareTo(other.key);
        }
    }

    private void buildJson(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.currentToken();
        if (token == null) {
            throw new JsonParseException(parser, "Unexpected null token");
        }
        switch (token) {
            case START_OBJECT -> {
                ArrayList<FieldEntry> fields = new ArrayList<>();
                int start = writePos;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String key = parser.currentName();
                    parser.nextToken();
                    int id = addKey(key);
                    fields.add(new FieldEntry(key, id, writePos - start));
                    buildJson(parser);
                }
                finishWritingObject(start, fields);
            }
            case START_ARRAY -> {
                ArrayList<Integer> offsets = new ArrayList<>();
                int start = writePos;
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    offsets.add(writePos - start);
                    buildJson(parser);
                }
                finishWritingArray(start, offsets);
            }
            case VALUE_STRING -> appendString(parser.getText());
            case VALUE_NUMBER_INT -> {
                try {
                    appendLong(parser.getLongValue());
                }
                catch (InputCoercionException _) {
                    // If the value doesn't fit any integer type, parse it as decimal or floating instead.
                    parseFloatingPoint(parser);
                }
            }
            case VALUE_NUMBER_FLOAT -> parseFloatingPoint(parser);
            case VALUE_TRUE -> appendBoolean(true);
            case VALUE_FALSE -> appendBoolean(false);
            case VALUE_NULL -> appendNull();
            default -> throw new JsonParseException(parser, "Unexpected token " + token);
        }
    }

    // Choose the smallest unsigned integer type that can store `value`. It must be within
    // `[0, U24_MAX]`.
    private int getIntegerSize(int value)
    {
        checkState(value >= 0 && value <= U24_MAX);
        if (value <= U8_MAX) {
            return 1;
        }
        if (value <= U16_MAX) {
            return 2;
        }
        return U24_SIZE;
    }

    private void parseFloatingPoint(JsonParser parser)
            throws IOException
    {
        if (!tryParseDecimal(parser.getText())) {
            appendDouble(parser.getDoubleValue());
        }
    }

    // Try to parse a JSON number as a decimal. Return whether the parsing succeeds. The input must
    // only use the decimal format (an integer value with an optional '.' in it) and must not use
    // scientific notation. It also must fit into the precision limitation of decimal types.
    private boolean tryParseDecimal(String input)
    {
        for (int i = 0; i < input.length(); ++i) {
            char ch = input.charAt(i);
            if (ch != '-' && ch != '.' && !(ch >= '0' && ch <= '9')) {
                return false;
            }
        }
        BigDecimal d = new BigDecimal(input);
        if (d.scale() <= MAX_DECIMAL16_PRECISION && d.precision() <= MAX_DECIMAL16_PRECISION) {
            appendDecimal(d);
            return true;
        }
        return false;
    }

    // The write buffer in building the variant value. Its first `writePos` bytes has been written.
    private byte[] writeBuffer = new byte[128];
    private int writePos;
    // Map keys to a monotonically increasing id.
    private final HashMap<String, Integer> dictionary = new HashMap<>();
    // Store all keys in `dictionary` in the order of id.
    private final ArrayList<byte[]> dictionaryKeys = new ArrayList<>();

    public static class VariantSizeLimitException
            extends RuntimeException {}
}
