/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;

public abstract class AbstractLanguageModelClient
        implements LanguageModelClient
{
    protected static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);
    protected static final JsonCodec<String> STRING_CODEC = jsonCodec(String.class);
    private static final PromptCodec NUMBERED_LIST_CODEC = new NumberedListCodec();
    private static final PromptCodec XML_TAGS_CODEC = new XmlTagsCodec();
    protected static final Logger log = Logger.get(AbstractLanguageModelClient.class);
    private static final int MAX_BATCH_SIZE = 32;
    private static final int MAX_BATCH_TEXT_LENGTH = 64_000;

    private final Executor executor;
    protected final PromptDao promptDao;
    protected final List<String> topLevelSystemPrompts;
    protected final ModelWithFixedPrompt analyzeSentimentModelAndPrompt;
    protected final ModelWithFixedPrompt analyzeSentimentModelAndPromptBatch;
    protected final ModelWithFixedPrompt classifyModelAndPrompt;
    protected final ModelWithFixedPrompt classifyModelAndPromptBatch;
    protected final ModelWithFixedPrompt fixGrammarModelAndPrompt;
    protected final ModelWithFixedPrompt fixGrammarModelAndPromptBatch;
    protected final ModelWithFixedPrompt maskModelAndPrompt;
    protected final ModelWithFixedPrompt maskModelAndPromptBatch;
    protected final ModelWithFixedPrompt translateModelAndPrompt;
    protected final ModelWithFixedPrompt translateModelAndPromptBatch;
    protected final ModelWithFixedPrompt summarizeModelAndPrompt;
    protected final ModelWithFixedPrompt summarizeModelAndPromptBatch;

    protected AbstractLanguageModelClient(PromptDao promptDao, Executor executor, int batchParallelism)
    {
        this.promptDao = requireNonNull(promptDao, "promptDao is null");
        this.topLevelSystemPrompts = promptDao.systemPrompts();
        this.analyzeSentimentModelAndPrompt = create("analyzeSentiment", topLevelSystemPrompts, promptDao.analyzeSentimentPrompt(), promptDao.analyzeSentimentSystemPrompt());
        this.analyzeSentimentModelAndPromptBatch = create("analyzeSentimentBatch", topLevelSystemPrompts, promptDao.analyzeSentimentPromptBatch(), Optional.empty());
        this.classifyModelAndPrompt = create("classify", topLevelSystemPrompts, promptDao.classifyPrompt(), promptDao.classifySystemPrompt());
        this.classifyModelAndPromptBatch = create("classifyBatch", topLevelSystemPrompts, promptDao.classifyPromptBatch(), promptDao.classifySystemPrompt());
        this.fixGrammarModelAndPrompt = create("fixGrammar", topLevelSystemPrompts, promptDao.fixGrammarPrompt(), promptDao.fixGrammarSystemPrompt());
        this.fixGrammarModelAndPromptBatch = create("fixGrammarBatch", topLevelSystemPrompts, promptDao.fixGrammarPromptBatch(), promptDao.fixGrammarSystemPrompt());
        this.maskModelAndPrompt = create("mask", topLevelSystemPrompts, promptDao.maskPrompt(), promptDao.maskSystemPrompt());
        this.maskModelAndPromptBatch = create("maskBatch", topLevelSystemPrompts, promptDao.maskPromptBatch(), promptDao.maskSystemPrompt());
        this.translateModelAndPrompt = create("translate", topLevelSystemPrompts, promptDao.translatePrompt(), promptDao.translateSystemPrompt());
        this.translateModelAndPromptBatch = create("translateBatch", topLevelSystemPrompts, promptDao.translatePromptBatch(), promptDao.translateSystemPrompt());
        this.summarizeModelAndPrompt = create("summarize", topLevelSystemPrompts, promptDao.summarizePrompt(), promptDao.summarizeSystemPrompt());
        this.summarizeModelAndPromptBatch = create("summarizeBatch", topLevelSystemPrompts, promptDao.summarizePromptBatch(), promptDao.summarizeSystemPrompt());
        this.executor = new BoundedExecutor(requireNonNull(executor, "executor is null"), batchParallelism);
    }

    @Override
    public String analyzeSentiment(String text)
    {
        return fixedCompletion(analyzeSentimentModelAndPrompt, analyzeSentimentModelAndPrompt.prompt().formatted(text));
    }

    @Override
    public List<String> analyzeSentimentBatch(List<String> texts)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, NUMBERED_LIST_CODEC,
                input -> fixedCompletion(analyzeSentimentModelAndPromptBatch, analyzeSentimentModelAndPromptBatch.prompt().formatted(input)));
    }

    @Override
    public String classify(String text, List<String> labels)
    {
        return fixedCompletion(classifyModelAndPrompt, formatLabelsAndText(classifyModelAndPrompt.prompt(), labels, text));
    }

    @Override
    public List<String> classifyBatch(List<String> texts, List<String> labels)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, NUMBERED_LIST_CODEC,
                input -> fixedCompletion(classifyModelAndPromptBatch, classifyModelAndPromptBatch.prompt().formatted(LIST_CODEC.toJson(labels), input)));
    }

    @Override
    public String fixGrammar(String text)
    {
        return fixedCompletion(fixGrammarModelAndPrompt, fixGrammarModelAndPrompt.prompt().formatted(text));
    }

    @Override
    public List<String> fixGrammarBatch(List<String> texts)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, XML_TAGS_CODEC,
                input -> fixedCompletion(fixGrammarModelAndPromptBatch, fixGrammarModelAndPromptBatch.prompt().formatted(input)));
    }

    @Override
    public String generate(String prompt)
    {
        return completion(prompt, Optional.empty());
    }

    @Override
    public String generate(String systemPrompt, String prompt)
    {
        return completion(prompt, Optional.of(systemPrompt));
    }

    @Override
    public String generate(List<LlmMessage> messages)
    {
        return generateCompletion(topLevelSystemPrompts, messages);
    }

    @Override
    public String generate(String systemPrompt, List<LlmMessage> messages)
    {
        return generateCompletion(
                ImmutableList.<String>builder()
                        .addAll(topLevelSystemPrompts)
                        .add(systemPrompt)
                        .build(),
                messages);
    }

    @Override
    public ToolUseResponse generateWithTools(String systemPrompt, List<LlmMessage> messages, List<ToolDefinition<?>> tools)
    {
        List<String> systemPrompts = ImmutableList.<String>builder()
                .addAll(topLevelSystemPrompts)
                .add(systemPrompt)
                .build();
        return generateCompletionWithTools(systemPrompts, messages, tools);
    }

    @Override
    public ToolUseResponse generateWithTools(String systemPrompt, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled)
    {
        List<String> systemPrompts = ImmutableList.<String>builder()
                .addAll(topLevelSystemPrompts)
                .add(systemPrompt)
                .build();
        return generateCompletionWithTools(systemPrompts, messages, tools, output, isCancelled);
    }

    @Override
    public String mask(String text, List<String> labels)
    {
        return fixedCompletion(maskModelAndPrompt, formatLabelsAndText(maskModelAndPrompt.prompt(), labels, text));
    }

    @Override
    public List<String> maskBatch(List<String> texts, List<String> labels)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, XML_TAGS_CODEC,
                input -> fixedCompletion(maskModelAndPromptBatch, maskModelAndPromptBatch.prompt().formatted(LIST_CODEC.toJson(labels), input)));
    }

    @Override
    public String translate(String text, String language)
    {
        return fixedCompletion(translateModelAndPrompt, translateModelAndPrompt.prompt().formatted(STRING_CODEC.toJson(language), text));
    }

    @Override
    public List<String> translateBatch(List<String> texts, String language)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, XML_TAGS_CODEC,
                input -> fixedCompletion(translateModelAndPromptBatch, translateModelAndPromptBatch.prompt().formatted(STRING_CODEC.toJson(language), input)));
    }

    @Override
    public String summarize(String text)
    {
        return fixedCompletion(summarizeModelAndPrompt, summarizeModelAndPrompt.prompt().formatted(text));
    }

    @Override
    public List<String> summarizeBatch(List<String> texts)
    {
        return batchInvocation(texts, MAX_BATCH_SIZE, MAX_BATCH_TEXT_LENGTH, XML_TAGS_CODEC,
                input -> fixedCompletion(summarizeModelAndPromptBatch, summarizeModelAndPromptBatch.prompt().formatted(input)));
    }

    private String completion(String prompt, Optional<String> system)
    {
        List<String> systemPrompts;
        if (system.isPresent()) {
            systemPrompts = ImmutableList.<String>builder()
                    .addAll(topLevelSystemPrompts)
                    .add(system.get())
                    .build();
        }
        else {
            systemPrompts = topLevelSystemPrompts;
        }
        return generateCompletion(systemPrompts, prompt);
    }

    private String fixedCompletion(ModelWithFixedPrompt modelAndPrompt, String userPrompt)
    {
        return completion(modelAndPrompt.systemPrompts(), userPrompt);
    }

    private String completion(List<String> systemPrompts, String userPrompt)
    {
        return generateCompletion(systemPrompts, userPrompt);
    }

    private static String formatLabelsAndText(String template, List<String> labels, String text)
    {
        return template.formatted(LIST_CODEC.toJson(labels), text);
    }

    protected abstract String generateCompletion(List<String> systemPrompts, String prompt);

    protected abstract String generateCompletion(List<String> systemPrompts, List<LlmMessage> llmMessages);

    protected abstract ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools);

    protected abstract ToolUseResponse generateCompletionWithTools(
            List<String> systemPrompts,
            List<LlmMessage> messages,
            List<ToolDefinition<?>> tools,
            Consumer<String> output,
            Supplier<Boolean> isCancelled);

    protected record ModelWithFixedPrompt(String name, List<String> systemPrompts, String prompt) {}

    protected static ModelWithFixedPrompt create(String name, List<String> topLevelSystemPrompts, String userPrompt, Optional<String> systemPrompt)
    {
        List<String> systemPrompts;
        if (systemPrompt.isPresent()) {
            systemPrompts = ImmutableList.<String>builder()
                    .addAll(topLevelSystemPrompts)
                    .add(systemPrompt.get())
                    .build();
        }
        else {
            systemPrompts = topLevelSystemPrompts;
        }
        return new ModelWithFixedPrompt(name, systemPrompts, userPrompt);
    }

    private List<String> batchInvocation(List<String> inputs, int maxBatchSize, int maxChars, PromptCodec codec, Function<String, String> modelInvocation)
    {
        List<CompletableFuture<List<String>>> batchFutures = new ArrayList<>();
        for (BatchedInputs batch : batchInputs(inputs, maxBatchSize, maxChars, codec)) {
            batchFutures.add(CompletableFuture.supplyAsync(
                    () -> callWithRetry(modelInvocation, batch, codec),
                    executor));
        }

        return batchFutures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    private List<String> callWithRetry(Function<String, String> function, BatchedInputs batch, PromptCodec codec)
    {
        try {
            String response = function.apply(batch.text());
            List<String> res = codec.decode(response, batch.startIndex());
            if (res.size() != batch.count()) {
                log.warn("Error processing LLM batch: expected %s results, but got %s;\nbatch was %s\nresponse was %s".formatted(batch.count(), res.size(), batch.text(), response));
                throw new LlmResponseException("Error processing LLM batch: expected %s results but got %s".formatted(batch.count(), res.size()));
            }
            return res;
        }
        catch (LlmResponseException e) {
            if (batch.count() > 4) {
                log.warn(e, "Batching failure at size: %s; splitting in half", batch.count());
                return batch.halfBatches().stream()
                        .map(halfBatch -> callWithRetry(function, halfBatch, codec))
                        .flatMap(List::stream)
                        .collect(toImmutableList());
            }
            else {
                log.warn(e, "Batching failure at size: %s; processing solo requests", batch.count());
                return batch.soloBatches().stream()
                        .map(soloBatch -> callWithRetry(function, soloBatch, codec))
                        .flatMap(List::stream)
                        .collect(toImmutableList());
            }
        }
    }

    private record BatchedInputs(List<String> inputs, int startIndex)
    {
        String text()
        {
            return String.join("", inputs);
        }

        int count()
        {
            return inputs.size();
        }

        List<BatchedInputs> soloBatches()
        {
            ImmutableList.Builder<BatchedInputs> builder = ImmutableList.builder();
            for (int index = 0; index < inputs.size(); index++) {
                builder.add(new BatchedInputs(ImmutableList.of(inputs.get(index)), startIndex + index));
            }
            return builder.build();
        }

        List<BatchedInputs> halfBatches()
        {
            int mid = inputs.size() / 2;
            return ImmutableList.of(
                    new BatchedInputs(inputs.subList(0, mid), startIndex),
                    new BatchedInputs(inputs.subList(mid, inputs.size()), startIndex + mid));
        }
    }

    private static Iterable<BatchedInputs> batchInputs(List<String> inputs, int maxInputsPerPrompt, int maxCharsPerPrompt, PromptCodec promptCodec)
    {
        return () -> new BatchIterator(inputs, maxInputsPerPrompt, maxCharsPerPrompt, promptCodec);
    }

    private static class BatchIterator
            implements Iterator<BatchedInputs>
    {
        private final List<String> inputs;
        private final int maxBatchSize;
        private final int maxChars;
        private final PromptCodec promptCodec;
        private int currentIndex;

        BatchIterator(List<String> inputs, int maxBatchSize, int maxChars, PromptCodec promptCodec)
        {
            this.inputs = requireNonNull(inputs, "inputs is null");
            checkArgument(maxBatchSize > 0, "maxBatchSize must be > 0");
            this.maxBatchSize = maxBatchSize;
            checkArgument(maxChars > 0, "maxChars must be > 0");
            this.maxChars = maxChars;
            this.promptCodec = requireNonNull(promptCodec, "promptCodec is null");
            currentIndex = 0;
        }

        @Override
        public boolean hasNext()
        {
            return currentIndex < inputs.size();
        }

        @Override
        public BatchedInputs next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("No more batches available");
            }

            ImmutableList.Builder<String> batch = ImmutableList.builder();
            int itemsInBatch = 0;
            int characterCount = 0;

            while (currentIndex < inputs.size() && itemsInBatch < maxBatchSize) {
                String text = promptCodec.encode(inputs.get(currentIndex), itemsInBatch + 1);
                if (itemsInBatch > 0 && characterCount + text.length() > maxChars) {
                    break;
                }
                batch.add(text);
                characterCount += text.length();
                itemsInBatch++;
                currentIndex++;
            }

            return new BatchedInputs(batch.build(), 1);
        }
    }
}
