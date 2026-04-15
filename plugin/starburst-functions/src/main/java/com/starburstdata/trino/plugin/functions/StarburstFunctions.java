/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.ai.CachingAiModelAccessControl;
import com.starburstdata.trino.plugin.functions.ai.embedding.GenerateEmbeddingsFunctionHandle;
import com.starburstdata.trino.plugin.functions.ai.embedding.GenerateEmbeddingsTableFunction;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.starburst.ai.client.ModelClientProvider;
import io.starburst.ai.client.TokenUsageContext;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProvider;
import io.trino.plugin.hive.HiveFileWriterFactory;
import io.trino.plugin.hive.HiveWriterStats;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.functions.Unload.UnloadFunctionHandle;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BatchFunctionImplementation;
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
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.trino.plugin.hive.functions.Unload.getUnloadFunctionProcessorProvider;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class StarburstFunctions
        implements FunctionProvider
{
    private static final TypeSignature TEXT = VARCHAR.getTypeSignature();
    private static final TypeSignature DOUBLE = DoubleType.DOUBLE.getTypeSignature();
    private static final List<FunctionMetadata> FUNCTIONS = ImmutableList.<FunctionMetadata>builder()
            .add(function("generate_embedding")
                    .description("Generate a vector embedding for the provided VARCHAR, using the specified model")
                    .signature(signature(TypeSignature.arrayType(DOUBLE), TEXT, TEXT))
                    .nullable()
                    .build())
            .add(function("generate_binary_embedding", "generate_binary_embedding")
                    .description("Generate a vector embedding for the provided VARCHAR with a VARBINARY encoding")
                    .signature(signature(VARBINARY.getTypeSignature(), TEXT, TEXT))
                    .nullable()
                    .build())
            .add(function("analyze_sentiment")
                    .description("Perform sentiment analysis on text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("analyze_sentiment")
                    .description("Perform sentiment analysis on text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("classify")
                    .description("Classify text with the provided labels, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("classify")
                    .description("Classify text with the provided labels, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(function("fix_grammar")
                    .description("Correct grammatical errors in text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("fix_grammar")
                    .description("Correct grammatical errors in text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("prompt")
                    .description("Generate text based on a prompt, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nullable()
                    .nondeterministic()
                    .build())
            .add(function("prompt")
                    .description("Generate text based on a system and user prompt, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT, TEXT))
                    .nullable()
                    .nondeterministic()
                    .build())
            .add(function("mask")
                    .description("Mask values for the provided labels in text, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("mask")
                    .description("Mask values for the provided labels in text, using the specified model")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT), TEXT))
                    .nondeterministic()
                    .build())
            .add(function("translate")
                    .description("Translate text to the specified language, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("translate")
                    .description("Translate text to the specified language, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(function("summarize")
                    .description("Summarize text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .add(batchFunction("summarize")
                    .description("Summarize text, using the specified model")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .nondeterministic()
                    .build())
            .build();

    private static final MethodHandle GENERATE_EMBEDDING;
    private static final MethodHandle GENERATE_BINARY_EMBEDDING;
    private static final MethodHandle ANALYZE_SENTIMENT;
    private static final MethodHandle ANALYZE_SENTIMENT_BATCH;
    private static final MethodHandle CLASSIFY;
    private static final MethodHandle CLASSIFY_BATCH;
    private static final MethodHandle FIX_GRAMMAR;
    private static final MethodHandle FIX_GRAMMAR_BATCH;
    private static final MethodHandle PROMPT;
    private static final MethodHandle PROMPT_SYSTEM;
    private static final MethodHandle MASK;
    private static final MethodHandle MASK_BATCH;
    private static final MethodHandle TRANSLATE;
    private static final MethodHandle TRANSLATE_BATCH;
    private static final MethodHandle SUMMARIZE;
    private static final MethodHandle SUMMARIZE_BATCH;

    static {
        try {
            GENERATE_EMBEDDING = lookup().findVirtual(StarburstFunctions.class, "generateEmbedding", methodType(Block.class, ConnectorSession.class, Slice.class, Slice.class));
            GENERATE_BINARY_EMBEDDING = lookup().findVirtual(StarburstFunctions.class, "generateBinaryEmbedding", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class));
            ANALYZE_SENTIMENT = lookup().findVirtual(StarburstFunctions.class, "analyzeSentiment", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class));
            ANALYZE_SENTIMENT_BATCH = lookup().findVirtual(StarburstFunctions.class, "analyzeSentimentBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
            CLASSIFY = lookup().findVirtual(StarburstFunctions.class, "classify", methodType(Slice.class, ConnectorSession.class, Slice.class, Block.class, Slice.class));
            CLASSIFY_BATCH = lookup().findVirtual(StarburstFunctions.class, "classifyBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ArrayBlock.class, int[].class, ValueBlock.class, int[].class));
            FIX_GRAMMAR = lookup().findVirtual(StarburstFunctions.class, "fixGrammar", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class));
            FIX_GRAMMAR_BATCH = lookup().findVirtual(StarburstFunctions.class, "fixGrammarBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
            PROMPT = lookup().findVirtual(StarburstFunctions.class, "prompt", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class));
            PROMPT_SYSTEM = lookup().findVirtual(StarburstFunctions.class, "promptSystem", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class, Slice.class));
            MASK = lookup().findVirtual(StarburstFunctions.class, "mask", methodType(Slice.class, ConnectorSession.class, Slice.class, Block.class, Slice.class));
            MASK_BATCH = lookup().findVirtual(StarburstFunctions.class, "maskBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ArrayBlock.class, int[].class, ValueBlock.class, int[].class));
            TRANSLATE = lookup().findVirtual(StarburstFunctions.class, "translate", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class, Slice.class));
            TRANSLATE_BATCH = lookup().findVirtual(StarburstFunctions.class, "translateBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
            SUMMARIZE = lookup().findVirtual(StarburstFunctions.class, "summarize", methodType(Slice.class, ConnectorSession.class, Slice.class, Slice.class));
            SUMMARIZE_BATCH = lookup().findVirtual(StarburstFunctions.class, "summarizeBatch", methodType(Block.class, ConnectorSession.class, ValueBlock.class, int[].class, ValueBlock.class, int[].class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final ModelClientProvider clientProvider;
    private final AiModelAccessControl accessControl;
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final HiveWriterStats hiveWriterStats;
    private final PageIndexerFactory pageIndexerFactory;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    @Inject
    public StarburstFunctions(
            ModelClientProvider clientProvider,
            AiModelAccessControl accessControl,
            Set<HiveFileWriterFactory> fileWriterFactories,
            HiveWriterStats hiveWriterStats,
            PageIndexerFactory pageIndexerFactory,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.clientProvider = requireNonNull(clientProvider, "clientProvider is null");
        this.accessControl = new CachingAiModelAccessControl(requireNonNull(accessControl, "accessControl is null"));
        this.fileWriterFactories = ImmutableSet.copyOf(fileWriterFactories);
        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
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
            case "generate_binary_embedding" -> GENERATE_BINARY_EMBEDDING;
            case "analyze_sentiment" -> ANALYZE_SENTIMENT;
            case "classify" -> CLASSIFY;
            case "fix_grammar" -> FIX_GRAMMAR;
            case "prompt" -> {
                if (boundSignature.getArity() == 2) {
                    yield PROMPT;
                }
                if (boundSignature.getArity() == 3) {
                    yield PROMPT_SYSTEM;
                }
                throw new IllegalArgumentException("Invalid number of arguments for function: " + name);
            }
            case "mask" -> MASK;
            case "translate" -> TRANSLATE;
            case "summarize" -> SUMMARIZE;
            default -> throw new IllegalArgumentException("Invalid function ID: " + functionId);
        };

        InvocationConvention.InvocationReturnConvention returnConvention = switch (name) {
            case "generate_embedding" -> NULLABLE_RETURN;
            case "generate_binary_embedding" -> NULLABLE_RETURN;
            case "prompt" -> NULLABLE_RETURN;
            default -> FAIL_ON_NULL;
        };

        handle = handle.bindTo(this);

        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(boundSignature.getArity(), NEVER_NULL),
                returnConvention,
                true,
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
    public BatchFunctionImplementation getBatchFunctionImplementation(FunctionId functionId)
    {
        String name = functionId.toString();
        MethodHandle handle = switch (name) {
            case "analyze_sentiment" -> ANALYZE_SENTIMENT_BATCH;
            case "classify" -> CLASSIFY_BATCH;
            case "fix_grammar" -> FIX_GRAMMAR_BATCH;
            case "mask" -> MASK_BATCH;
            case "translate" -> TRANSLATE_BATCH;
            case "summarize" -> SUMMARIZE_BATCH;
            default -> throw new IllegalArgumentException("Invalid function ID for batch function: " + functionId);
        };
        handle = handle.bindTo(this);
        return new BatchFunctionImplementation(handle);
    }

    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof GenerateEmbeddingsFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(
                    GenerateEmbeddingsTableFunction.getGenerateEmbeddingsFunctionProcessorProvider(clientProvider),
                    getClass().getClassLoader());
        }
        if (functionHandle instanceof UnloadFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProvider(getUnloadFunctionProcessorProvider(
                    fileWriterFactories,
                    hiveWriterStats,
                    pageIndexerFactory,
                    partitionUpdateCodec),
                    getClass().getClassLoader());
        }
        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }

    private Block generateEmbedding(ConnectorSession session, Slice sourceString, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

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
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to generate embedding with remote model", e);
        }

        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, data.size());
        for (Double datum : data) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, datum);
        }
        return blockBuilder.build();
    }

    private Slice generateBinaryEmbedding(ConnectorSession session, Slice sourceString, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        if (sourceString.length() == 0) {
            return null;
        }

        try {
            return clientProvider.embeddingModelClient(modelId).generateBinaryEmbedding(sourceString);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to generate embedding with remote model", e);
        }
    }

    public Slice analyzeSentiment(ConnectorSession session, Slice text, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        return utf8Slice(clientProvider.languageModelClient(modelId).analyzeSentiment(text.toStringUtf8()));
    }

    public Block analyzeSentimentBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<?> group : batchByModelId(textBlock, true, textPositions, new DummyBlockHandler(), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).analyzeSentimentBatch(group.texts());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
    }

    public Slice classify(ConnectorSession session, Slice text, Block labels, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        return utf8Slice(clientProvider.languageModelClient(modelId).classify(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Block classifyBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ArrayBlock labelsBlock, int[] labelsPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<List<String>> group : batchByModelId(textBlock, false, textPositions, new ArrayBlockHandler(labelsBlock, labelsPositions), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).classifyBatch(group.texts(), group.additionalArgument());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
    }

    public Slice fixGrammar(ConnectorSession session, Slice text, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        return utf8Slice(clientProvider.languageModelClient(modelId).fixGrammar(text.toStringUtf8()));
    }

    public Block fixGrammarBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<?> group : batchByModelId(textBlock, true, textPositions, new DummyBlockHandler(), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).fixGrammarBatch(group.texts());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
    }

    public Slice prompt(ConnectorSession session, Slice prompt, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        if (prompt.length() == 0) {
            return null;
        }
        return utf8Slice(clientProvider.languageModelClient(modelId).generate(prompt.toStringUtf8(), TokenUsageContext.EMPTY));
    }

    public Slice promptSystem(ConnectorSession session, Slice systemPrompt, Slice prompt, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        if (prompt.length() == 0) {
            return null;
        }
        return utf8Slice(clientProvider.languageModelClient(modelId).generate(prompt.toStringUtf8(), systemPrompt.toStringUtf8(), TokenUsageContext.EMPTY));
    }

    public Slice mask(ConnectorSession session, Slice text, Block labels, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        return utf8Slice(clientProvider.languageModelClient(modelId).mask(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Block maskBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ArrayBlock labelsBlock, int[] labelsPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<List<String>> group : batchByModelId(textBlock, false, textPositions, new ArrayBlockHandler(labelsBlock, labelsPositions), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).maskBatch(group.texts(), group.additionalArgument());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
    }

    public Slice translate(ConnectorSession session, Slice text, Slice language, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        return utf8Slice(clientProvider.languageModelClient(modelId).translate(text.toStringUtf8(), language.toStringUtf8()));
    }

    public Block translateBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ValueBlock labelsBlock, int[] labelsPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<String> group : batchByModelId(textBlock, false, textPositions, new SliceBlockHandler(labelsBlock, labelsPositions), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).translateBatch(group.texts(), group.additionalArgument());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
    }

    public Slice summarize(ConnectorSession session, Slice text, Slice modelId)
    {
        accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());
        return utf8Slice(clientProvider.languageModelClient(modelId).summarize(text.toStringUtf8()));
    }

    public Block summarizeBatch(ConnectorSession session, ValueBlock textBlock, int[] textPositions, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        int length = textPositions.length;
        BlockBuilder resultsBlockBuilder = VARCHAR.createBlockBuilder(null, length);

        for (ModelGroupInputs<Object> group : batchByModelId(textBlock, false, textPositions, new DummyBlockHandler(), modelIdBlock, modelIdPositions)) {
            Slice modelId = group.modelId();

            accessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

            List<String> results = clientProvider.languageModelClient(modelId).summarizeBatch(group.texts());
            addResults(results, resultsBlockBuilder, group);
        }

        return resultsBlockBuilder.build();
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

    private static FunctionMetadata.Builder batchFunction(String name)
    {
        return FunctionMetadata.batchBuilder(name).functionId(new FunctionId(name));
    }

    private static Signature signature(TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return Signature.builder()
                .returnType(returnType)
                .argumentTypes(List.of(argumentTypes))
                .build();
    }

    private static void addResults(List<String> results, BlockBuilder resultsBlockBuilder, ModelGroupInputs<?> group)
    {
        Iterator<String> resultIter = results.iterator();
        for (boolean nullOrEmpty : group.nullOrEmpty()) {
            if (nullOrEmpty) {
                resultsBlockBuilder.appendNull();
            }
            else {
                VARCHAR.writeSlice(resultsBlockBuilder, utf8Slice(resultIter.next()));
            }
        }
    }

    private static <T, U> Iterable<ModelGroupInputs<U>> batchByModelId(
            ValueBlock textBlock, boolean treatEmptyAsNull, int[] textPositions, ArgumentBlockHandler<T, U> argumentBlockHandler, ValueBlock modelIdBlock, int[] modelIdPositions)
    {
        return () -> new ModelGroupIterator<>(textBlock, treatEmptyAsNull, textPositions, argumentBlockHandler, modelIdBlock, modelIdPositions);
    }

    /**
     * Extracts contiguous sequences that share a common model and potentially another argument (like language or labels).
     */
    private static class ModelGroupIterator<T, U>
            implements Iterator<ModelGroupInputs<U>>
    {
        private final ValueBlock textBlock;
        private final boolean treatEmptyAsNull;
        private final int[] textPositions;
        private final ArgumentBlockHandler<T, U> argumentBlockHandler;
        private final ValueBlock modelIdBlock;
        private final int[] modelIdPositions;
        private int currentIndex;

        public ModelGroupIterator(ValueBlock textBlock, boolean treatEmptyAsNull, int[] textPositions, ArgumentBlockHandler<T, U> argumentBlockHandler, ValueBlock modelIdBlock, int[] modelIdPositions)
        {
            this.textBlock = textBlock;
            this.treatEmptyAsNull = treatEmptyAsNull;
            this.textPositions = textPositions;
            this.argumentBlockHandler = argumentBlockHandler;
            this.modelIdBlock = modelIdBlock;
            this.modelIdPositions = modelIdPositions;
            this.currentIndex = 0;
        }

        @Override
        public boolean hasNext()
        {
            return currentIndex < textPositions.length;
        }

        @Override
        public ModelGroupInputs<U> next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            int startIndex = currentIndex;
            Slice currentModelId = getModelIdAt(currentIndex);
            T currentAdditionalArgValue = argumentBlockHandler.valueAt(currentIndex);

            // Find the end of the current group
            while (currentIndex < textPositions.length &&
                    currentModelId.equals(getModelIdAt(currentIndex)) &&
                    argumentBlockHandler.valueAtIndexEquals(currentIndex, currentAdditionalArgValue)) {
                currentIndex++;
            }

            int groupSize = currentIndex - startIndex;
            ImmutableList.Builder<String> texts = ImmutableList.builder();
            boolean[] nullOrEmpty = new boolean[groupSize];

            for (int i = 0; i < groupSize; i++) {
                int globalIndex = startIndex + i;
                int textPosition = textPositions[globalIndex];

                if (textBlock.isNull(textPosition)) {
                    nullOrEmpty[i] = true;
                }
                else {
                    Slice value = VARCHAR.getSlice(textBlock, textPosition);
                    if (treatEmptyAsNull && value.length() == 0) {
                        nullOrEmpty[i] = true;
                    }
                    else {
                        texts.add(value.toStringUtf8());
                    }
                }
            }

            return new ModelGroupInputs<>(currentModelId, texts.build(), argumentBlockHandler.toJavaType(currentAdditionalArgValue), nullOrEmpty, startIndex, currentIndex - 1);
        }

        private Slice getModelIdAt(int index)
        {
            int modelIdPosition = modelIdPositions[index];
            if (modelIdBlock.isNull(modelIdPosition)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Model ID cannot be null");
            }
            return VARCHAR.getSlice(modelIdBlock, modelIdPosition);
        }
    }

    private interface ArgumentBlockHandler<T, U>
    {
        boolean valueAtIndexEquals(int index, T value);

        T valueAt(int index);

        U toJavaType(T value);
    }

    private static class ArrayBlockHandler
            implements ArgumentBlockHandler<Block, List<String>>
    {
        private final ArrayBlock arrayBlock;
        private final int[] arrayPositions;

        private ArrayBlockHandler(ArrayBlock arrayBlock, int[] arrayPositions)
        {
            this.arrayBlock = arrayBlock;
            this.arrayPositions = arrayPositions;
        }

        @Override
        public boolean valueAtIndexEquals(int index, Block value)
        {
            return arrayBlocksEqual(valueAt(index), value);
        }

        @Override
        public Block valueAt(int index)
        {
            int position = arrayPositions[index];
            if (arrayBlock.isNull(position)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Array argument cannot be null");
            }
            return arrayBlock.getArray(position);
        }

        @Override
        public List<String> toJavaType(Block value)
        {
            return fromSqlArray(value);
        }

        private boolean arrayBlocksEqual(Block block1, Block block2)
        {
            int positionCount = block1.getPositionCount();
            if (positionCount != block2.getPositionCount()) {
                return false;
            }

            // We treat the arrays as sets
            HashSet<Slice> set1 = new HashSet<>();
            HashSet<Slice> set2 = new HashSet<>();
            for (int i = 0; i < positionCount; i++) {
                if (block1.isNull(i) || block2.isNull(i)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Array values cannot be null");
                }

                set1.add(VARCHAR.getSlice(block1, i));
                set2.add(VARCHAR.getSlice(block2, i));
            }
            return set1.equals(set2);
        }
    }

    private static class SliceBlockHandler
            implements ArgumentBlockHandler<Slice, String>
    {
        private final ValueBlock block;
        private final int[] positions;

        private SliceBlockHandler(ValueBlock block, int[] positions)
        {
            this.block = block;
            this.positions = positions;
        }

        @Override
        public boolean valueAtIndexEquals(int index, Slice value)
        {
            return value.equals(valueAt(index));
        }

        @Override
        public Slice valueAt(int index)
        {
            int position = positions[index];
            return VARCHAR.getSlice(block, position);
        }

        @Override
        public String toJavaType(Slice value)
        {
            return value.toStringUtf8();
        }
    }

    private static class DummyBlockHandler
            implements ArgumentBlockHandler<Object, Object>
    {
        private static final Object DUMMY_VALUE = new Object();

        @Override
        public boolean valueAtIndexEquals(int index, Object value)
        {
            return true;
        }

        @Override
        public Object valueAt(int index)
        {
            return DUMMY_VALUE;
        }

        @Override
        public Object toJavaType(Object value)
        {
            return DUMMY_VALUE;
        }
    }

    /**
     * Result of a model group iteration, containing the model ID, the values and nulls,
     * and the start/end indices in the original arrays.
     */
    private record ModelGroupInputs<U>(
            Slice modelId, List<String> texts, U additionalArgument, boolean[] nullOrEmpty, int startIndex, int endIndex) {}
}
