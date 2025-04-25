/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.vbyte;

import io.starburst.vbyte.internal.NativeFunctionSignature;
import io.starburst.vbyte.internal.NativeLoader;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.vbyte.VByteUtils.maxIntsEncodedLength;
import static java.lang.invoke.MethodHandles.lookup;

public final class VByteNative
{
    public static final int SIZE_OF_INT = 4;
    public static final int SIZE_OF_LONG = 8;

    private static final Optional<LinkageError> LINKAGE_ERROR;
    private static final MethodHandle STREAM_VBYTE_ENCODE;
    private static final MethodHandle STREAM_VBYTE_DECODE;

    private record MethodHandles(
            // size_t streamvbyte_encode(const uint32_t* in, uint32_t length, uint8_t* out);
            @NativeFunctionSignature(name = "streamvbyte_encode_0124", returnType = long.class, argumentTypes = {MemorySegment.class, int.class, MemorySegment.class})
            MethodHandle streamVByteEncode,

            // size_t streamvbyte_decode(const uint8_t* in, uint32_t* out, uint32_t length);
            @NativeFunctionSignature(name = "streamvbyte_decode_0124", returnType = long.class, argumentTypes = {MemorySegment.class, MemorySegment.class, int.class})
            MethodHandle streamVByteDecode)
    {}

    private VByteNative() {}

    static {
        NativeLoader.Symbols<MethodHandles> symbols = NativeLoader.loadSymbols("streamvbyte", MethodHandles.class, lookup());
        LINKAGE_ERROR = symbols.linkageError();
        STREAM_VBYTE_ENCODE = symbols.symbols().streamVByteEncode();
        STREAM_VBYTE_DECODE = symbols.symbols().streamVByteDecode();
    }

    public static Optional<LinkageError> getLinkageError()
    {
        return LINKAGE_ERROR;
    }

    public static void verifyEnabled()
    {
        if (LINKAGE_ERROR.isPresent()) {
            throw new IllegalStateException("VByte native library is not enabled", LINKAGE_ERROR.get());
        }
    }

    public static int encode(MemorySegment input, int inputLength, MemorySegment output)
    {
        checkArgument(output.byteSize() >= maxIntsEncodedLength(inputLength), "Output buffer is too small");
        try {
            long encodedBytes = (long) STREAM_VBYTE_ENCODE.invokeExact(input, inputLength, output);
            return (int) encodedBytes;
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }

    public static int decode(MemorySegment input, int numberOfIntsToDecode, MemorySegment output)
    {
        checkArgument(output.byteSize() >= (long) numberOfIntsToDecode * SIZE_OF_INT, "Output buffer is too small");
        try {
            long decodedBytes = (long) STREAM_VBYTE_DECODE.invokeExact(input, output, numberOfIntsToDecode);
            return (int) decodedBytes;
        }
        catch (Throwable e) {
            throw new AssertionError("should not reach here", e);
        }
    }
}
