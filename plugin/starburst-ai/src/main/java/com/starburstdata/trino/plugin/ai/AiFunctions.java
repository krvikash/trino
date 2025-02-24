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
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.ai.embedding.GenerateEmbeddingsFunctionHandle;
import com.starburstdata.trino.plugin.ai.embedding.GenerateEmbeddingsTableFunction;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class AiFunctions
        implements FunctionProvider
{
    private static final TypeSignature TEXT = VARCHAR.getTypeSignature();
    private static final TypeSignature DOUBLE = DoubleType.DOUBLE.getTypeSignature();
    private static final List<FunctionMetadata> FUNCTIONS = ImmutableList.<FunctionMetadata>builder()
            .add(function("generate_embedding")
                    .description("Generate a vector embedding for the provided VARCHAR, using the specified model")
                    .signature(signature(TypeSignature.arrayType(DOUBLE), TEXT, TEXT))
                    .build())
            .add(function("analyze_sentiment")
                    .description("Perform sentiment analysis on text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("classify")
                    .description("Classify text with the provided labels, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(function("fix_grammar")
                    .description("Correct grammatical errors in text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("prompt")
                    .description("Generate text based on a prompt, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("prompt")
                    .description("Generate text based on a system and user prompt, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("mask")
                    .description("Mask values for the provided labels in text, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(function("translate")
                    .description("Translate text to the specified language, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .build();

    private static final MethodHandle GENERATE_EMBEDDING;
    private static final MethodHandle ANALYZE_SENTIMENT;
    private static final MethodHandle CLASSIFY;
    private static final MethodHandle FIX_GRAMMAR;
    private static final MethodHandle PROMPT;
    private static final MethodHandle PROMPT_SYSTEM;
    private static final MethodHandle MASK;
    private static final MethodHandle TRANSLATE;

    static {
        try {
            GENERATE_EMBEDDING = lookup().findVirtual(AiFunctions.class, "generateEmbedding", methodType(Block.class, Slice.class, Slice.class));
            ANALYZE_SENTIMENT = lookup().findVirtual(AiFunctions.class, "analyzeSentiment", methodType(Slice.class, Slice.class, Slice.class));
            CLASSIFY = lookup().findVirtual(AiFunctions.class, "classify", methodType(Slice.class, Slice.class, Block.class, Slice.class));
            FIX_GRAMMAR = lookup().findVirtual(AiFunctions.class, "fixGrammar", methodType(Slice.class, Slice.class, Slice.class));
            PROMPT = lookup().findVirtual(AiFunctions.class, "prompt", methodType(Slice.class, Slice.class, Slice.class));
            PROMPT_SYSTEM = lookup().findVirtual(AiFunctions.class, "promptSystem", methodType(Slice.class, Slice.class, Slice.class, Slice.class));
            MASK = lookup().findVirtual(AiFunctions.class, "mask", methodType(Slice.class, Slice.class, Block.class, Slice.class));
            TRANSLATE = lookup().findVirtual(AiFunctions.class, "translate", methodType(Slice.class, Slice.class, Slice.class, Slice.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final ClientProvider clientProvider;

    @Inject
    public AiFunctions(ClientProvider clientProvider)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
    }

    public List<FunctionMetadata> getFunctions()
    {
        return FUNCTIONS;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        String name = functionId.toString();
        MethodHandle handle = switch (name) {
            case "generate_embedding" -> GENERATE_EMBEDDING;
            case "analyze_sentiment" -> ANALYZE_SENTIMENT;
            case "classify" -> CLASSIFY;
            case "fix_grammar" -> FIX_GRAMMAR;
            case "prompt" -> {
                if (boundSignature.getArity() == 2) {
                    yield PROMPT;
                }
                else if (boundSignature.getArity() == 3) {
                    yield PROMPT_SYSTEM;
                }
                else {
                    throw new IllegalArgumentException("Invalid number of arguments for function: " + name);
                }
            }
            case "mask" -> MASK;
            case "translate" -> TRANSLATE;
            default -> throw new IllegalArgumentException("Invalid function ID: " + functionId);
        };

        handle = handle.bindTo(this);

        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(boundSignature.getArity(), NEVER_NULL),
                FAIL_ON_NULL,
                false,
                false);

        handle = ScalarFunctionAdapter.adapt(
                handle,
                boundSignature.getReturnType(),
                boundSignature.getArgumentTypes(),
                actualConvention,
                invocationConvention);

        return ScalarFunctionImplementation.builder()
                .methodHandle(handle)
                .build();
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof GenerateEmbeddingsFunctionHandle(Slice modelId)) {
            return GenerateEmbeddingsTableFunction.getGenerateEmbeddingsFunctionProcessorProvider(clientProvider.embeddingModelClient(modelId));
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }

    private Block generateEmbedding(Slice sourceString, Slice modelId)
    {
        if (sourceString.length() == 0) {
            return null;
        }

        List<Double> data;
        try {
            data = clientProvider.embeddingModelClient(modelId).generateEmbedding(sourceString);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AiErrorCode.AI_ERROR, "Failed to generate embedding with remote model", e);
        }

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, data.size());
        for (Double datum : data) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, datum);
        }
        return blockBuilder.build();
    }

    public Slice analyzeSentiment(Slice text, Slice modelId)
    {
        return utf8Slice(clientProvider.languageModelClient(modelId).analyzeSentiment(text.toStringUtf8()));
    }

    public Slice classify(Slice text, Block labels, Slice modelId)
    {
        return utf8Slice(clientProvider.languageModelClient(modelId).classify(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Slice fixGrammar(Slice text, Slice modelId)
    {
        return utf8Slice(clientProvider.languageModelClient(modelId).fixGrammar(text.toStringUtf8()));
    }

    public Slice prompt(Slice prompt, Slice modelId)
    {
        if (prompt.length() == 0) {
            return null;
        }
        return utf8Slice(clientProvider.languageModelClient(modelId).generate(prompt.toStringUtf8()));
    }

    public Slice promptSystem(Slice systemPrompt, Slice prompt, Slice modelId)
    {
        if (prompt.length() == 0) {
            return null;
        }
        return utf8Slice(clientProvider.languageModelClient(modelId).generate(prompt.toStringUtf8(), systemPrompt.toStringUtf8()));
    }

    public Slice mask(Slice text, Block labels, Slice modelId)
    {
        return utf8Slice(clientProvider.languageModelClient(modelId).mask(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Slice translate(Slice text, Slice language, Slice modelId)
    {
        return utf8Slice(clientProvider.languageModelClient(modelId).translate(text.toStringUtf8(), language.toStringUtf8()));
    }

    private static List<String> fromSqlArray(Block block)
    {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            list.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return list;
    }

    private static FunctionMetadata.Builder function(String name)
    {
        return function(name, name);
    }

    private static FunctionMetadata.Builder function(String name, String functionId)
    {
        return FunctionMetadata.scalarBuilder(name).functionId(new FunctionId(functionId));
    }

    private static Signature signature(TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return Signature.builder()
                .returnType(returnType)
                .argumentTypes(List.of(argumentTypes))
                .build();
    }
}
