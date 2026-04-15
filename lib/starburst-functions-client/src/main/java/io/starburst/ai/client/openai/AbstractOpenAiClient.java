/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai;

import com.google.common.collect.ImmutableList;
import com.openai.errors.BadRequestException;
import com.openai.errors.InternalServerException;
import com.openai.errors.NotFoundException;
import com.openai.errors.PermissionDeniedException;
import com.openai.errors.RateLimitException;
import com.openai.errors.UnauthorizedException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.PromptDao;
import io.starburst.ai.client.TokenUsage;
import io.starburst.ai.client.TokenUsageContext;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.client.ToolDefinition;
import io.starburst.ai.client.ToolUseResponse;
import io.trino.spi.TrinoException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.MessageRole.USER;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;

public abstract class AbstractOpenAiClient<ResponseType>
        extends AbstractLanguageModelClient
{
    protected static final int SEED = 37;

    private final boolean isToolStreamingSupported;
    private final RetryPolicy<ResponseType> retryPolicy = RetryPolicy.<ResponseType>builder()
            .handleIf(AbstractOpenAiClient::isRetryable)
            .withMaxRetries(4)
            .withBackoff(Duration.ofMillis(500), Duration.ofMinutes(2))
            .withJitter(0.25)
            .build();

    protected AbstractOpenAiClient(
            PromptDao promptDao,
            Executor executor,
            int batchParallelism,
            boolean isToolStreamingSupported,
            TokenUsageListener tokenUsageListener)
    {
        super(promptDao, executor, batchParallelism, tokenUsageListener);
        this.isToolStreamingSupported = isToolStreamingSupported;
    }

    @Override
    protected String generateCompletion(List<String> systemPrompts, String prompt, TokenUsageContext context)
    {
        return generateCompletion(systemPrompts, ImmutableList.of(new LlmMessage(USER, prompt)), context);
    }

    @Override
    protected ToolUseResponse generateCompletionWithTools(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled, TokenUsageContext context)
    {
        if (!isToolStreamingSupported) {
            ToolUseResponse response = generateCompletionWithTools(systemPrompts, messages, tools, context);
            output.accept(response.textResponse());
            return response;
        }
        StringBuilder streamedText = new StringBuilder();
        Consumer<String> trackingOutput = chunk -> {
            streamedText.append(chunk);
            output.accept(chunk);
        };
        ResponseType response = execute(() -> streamToolResponse(systemPrompts, messages, tools, trackingOutput, isCancelled), context);
        if (response == null) {
            return new ToolUseResponse("", ImmutableList.of());
        }
        ToolUseResponse toolUseResponse = parseToolResponse(response);
        // Ensure textResponse matches what was actually streamed to the caller
        if (toolUseResponse.textResponse().isBlank() && !streamedText.isEmpty()) {
            return new ToolUseResponse(streamedText.toString(), toolUseResponse.toolCalls());
        }
        return toolUseResponse;
    }

    protected abstract ResponseType streamToolResponse(List<String> systemPrompts, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled);

    protected final ResponseType execute(Supplier<ResponseType> call, TokenUsageContext context)
    {
        try {
            ResponseType response = Failsafe.with(retryPolicy).get(call::get);
            if (response != null) {
                extractTokenUsage(response).ifPresent(usage -> reportTokenUsage(context, usage));
            }
            return response;
        }
        catch (RuntimeException e) {
            throw toTrinoException(e);
        }
    }

    protected abstract Optional<TokenUsage> extractTokenUsage(ResponseType response);

    protected abstract ToolUseResponse parseToolResponse(ResponseType response);

    private static boolean isRetryable(Throwable t)
    {
        return t instanceof RateLimitException || t instanceof InternalServerException;
    }

    protected static TrinoException toTrinoException(Exception ex)
    {
        return switch (ex) {
            case BadRequestException e -> new TrinoException(INVALID_MODEL_CONFIGURATION, "OpenAI request failed validation", e);
            case NotFoundException e -> new TrinoException(AI_CLIENT_ERROR, "OpenAI model not found", e);
            case UnauthorizedException e -> new TrinoException(AI_CLIENT_ERROR, "Unauthorized response from OpenAI", e);
            case PermissionDeniedException e -> new TrinoException(PERMISSION_DENIED, "Permission to OpenAI API denied", e);
            case TrinoException e -> e;
            default -> new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request", ex);
        };
    }
}
