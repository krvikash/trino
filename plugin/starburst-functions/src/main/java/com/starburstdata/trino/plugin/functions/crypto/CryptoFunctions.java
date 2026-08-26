/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.crypto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeDescriptor;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

/**
 * SQL-visible metadata and method handles for the {@code crypto} schema. The implementations live in
 * {@link AesFunctions}.
 * <p>
 * Each overload gets its own {@link FunctionId} so that {@code getFunctionMetadata} can resolve a
 * single unambiguous entry, and the optional {@code iv} and {@code aad} arguments are declared
 * nullable so that passing {@code NULL} selects the default behaviour.
 */
public final class CryptoFunctions
{
    public static final String CRYPTO_SCHEMA_NAME = "crypto";

    private static final String AES_ENCRYPT = "aes_encrypt";
    private static final String AES_DECRYPT = "aes_decrypt";

    // Function id suffixes, one per overload, named after the arguments that overload accepts.
    private static final String ENCRYPT_ID = "aes_encrypt";
    private static final String ENCRYPT_MODE_ID = "aes_encrypt_mode";
    private static final String ENCRYPT_MODE_IV_ID = "aes_encrypt_mode_iv";
    private static final String ENCRYPT_MODE_IV_AAD_ID = "aes_encrypt_mode_iv_aad";
    private static final String DECRYPT_ID = "aes_decrypt";
    private static final String DECRYPT_MODE_ID = "aes_decrypt_mode";
    private static final String DECRYPT_MODE_IV_ID = "aes_decrypt_mode_iv";
    private static final String DECRYPT_MODE_IV_AAD_ID = "aes_decrypt_mode_iv_aad";

    private static final TypeDescriptor BINARY = VARBINARY.getTypeDescriptor();
    private static final TypeDescriptor TEXT = VARCHAR.getTypeDescriptor();

    private static final List<FunctionMetadata> FUNCTIONS = ImmutableList.<FunctionMetadata>builder()
            .add(function(AES_ENCRYPT, ENCRYPT_ID)
                    .description("Encrypt binary with AES-GCM under the given key")
                    .signature(signature(BINARY, BINARY, BINARY))
                    .argumentNullability(false, false)
                    .nondeterministic()
                    .build())
            .add(function(AES_ENCRYPT, ENCRYPT_MODE_ID)
                    .description("Encrypt binary with AES under the given key and mode (GCM, CBC)")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT))
                    .argumentNullability(false, false, false)
                    .nondeterministic()
                    .build())
            .add(function(AES_ENCRYPT, ENCRYPT_MODE_IV_ID)
                    .description("Encrypt binary with AES under the given key, mode (GCM, CBC), and initialization vector")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT, BINARY))
                    .argumentNullability(false, false, false, true)
                    .nondeterministic()
                    .build())
            .add(function(AES_ENCRYPT, ENCRYPT_MODE_IV_AAD_ID)
                    .description("Encrypt binary with AES under the given key, mode (GCM, CBC), initialization vector, and associated data")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT, BINARY, BINARY))
                    .argumentNullability(false, false, false, true, true)
                    .nondeterministic()
                    .build())
            .add(function(AES_DECRYPT, DECRYPT_ID)
                    .description("Decrypt binary produced by aes_encrypt")
                    .signature(signature(BINARY, BINARY, BINARY))
                    .argumentNullability(false, false)
                    .build())
            .add(function(AES_DECRYPT, DECRYPT_MODE_ID)
                    .description("Decrypt binary produced by aes_encrypt using the given mode (GCM, CBC)")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT))
                    .argumentNullability(false, false, false)
                    .build())
            .add(function(AES_DECRYPT, DECRYPT_MODE_IV_ID)
                    .description("Decrypt binary produced by aes_encrypt using the given mode and initialization vector")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT, BINARY))
                    .argumentNullability(false, false, false, true)
                    .build())
            .add(function(AES_DECRYPT, DECRYPT_MODE_IV_AAD_ID)
                    .description("Decrypt binary produced by aes_encrypt using the given mode, initialization vector, and associated data")
                    .signature(signature(BINARY, BINARY, BINARY, TEXT, BINARY, BINARY))
                    .argumentNullability(false, false, false, true, true)
                    .build())
            .build();

    private static final Map<FunctionId, MethodHandle> HANDLES;
    private static final Map<FunctionId, List<InvocationConvention.InvocationArgumentConvention>> ARGUMENT_CONVENTIONS;

    static {
        try {
            HANDLES = ImmutableMap.<FunctionId, MethodHandle>builder()
                    .put(functionId(ENCRYPT_ID), handle("aesEncrypt", 2))
                    .put(functionId(ENCRYPT_MODE_ID), handle("aesEncrypt", 3))
                    .put(functionId(ENCRYPT_MODE_IV_ID), handle("aesEncrypt", 4))
                    .put(functionId(ENCRYPT_MODE_IV_AAD_ID), handle("aesEncrypt", 5))
                    .put(functionId(DECRYPT_ID), handle("aesDecrypt", 2))
                    .put(functionId(DECRYPT_MODE_ID), handle("aesDecrypt", 3))
                    .put(functionId(DECRYPT_MODE_IV_ID), handle("aesDecrypt", 4))
                    .put(functionId(DECRYPT_MODE_IV_AAD_ID), handle("aesDecrypt", 5))
                    .buildOrThrow();
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }

        ARGUMENT_CONVENTIONS = FUNCTIONS.stream()
                .collect(toImmutableMap(
                        FunctionMetadata::getFunctionId,
                        function -> function.getFunctionNullability().getArgumentNullable().stream()
                                .map(nullable -> nullable ? BOXED_NULLABLE : NEVER_NULL)
                                .collect(toImmutableList())));
    }

    private CryptoFunctions() {}

    public static List<FunctionMetadata> getFunctions()
    {
        return FUNCTIONS;
    }

    public static boolean isCryptoFunction(FunctionId functionId)
    {
        return HANDLES.containsKey(functionId);
    }

    public static ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            InvocationConvention invocationConvention)
    {
        MethodHandle handle = HANDLES.get(functionId);
        if (handle == null) {
            throw new IllegalArgumentException("Invalid function ID: " + functionId);
        }

        InvocationConvention actualConvention = new InvocationConvention(
                ARGUMENT_CONVENTIONS.get(functionId),
                FAIL_ON_NULL,
                false,
                false);

        return ScalarFunctionImplementation.builder()
                .methodHandle(ScalarFunctionAdapter.adapt(
                        handle,
                        boundSignature.getReturnType(),
                        boundSignature.getArgumentTypes(),
                        actualConvention,
                        invocationConvention))
                .build();
    }

    private static MethodHandle handle(String methodName, int arity)
            throws ReflectiveOperationException
    {
        Class<?>[] parameterTypes = new Class<?>[arity];
        Arrays.fill(parameterTypes, Slice.class);
        return lookup().findStatic(AesFunctions.class, methodName, methodType(Slice.class, parameterTypes));
    }

    private static FunctionMetadata.Builder function(String canonicalName, String unqualifiedName)
    {
        return FunctionMetadata.scalarBuilder(canonicalName).functionId(functionId(unqualifiedName));
    }

    private static FunctionId functionId(String unqualifiedName)
    {
        return new FunctionId(CRYPTO_SCHEMA_NAME + "." + unqualifiedName);
    }

    private static Signature signature(TypeDescriptor returnType, TypeDescriptor... argumentTypes)
    {
        return Signature.builder()
                .returnType(returnType)
                .argumentTypes(List.of(argumentTypes))
                .build();
    }
}
