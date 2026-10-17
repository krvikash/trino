/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.StaticPromptDao;
import io.starburst.ai.client.TokenUsageContext;
import io.starburst.ai.client.TokenUsageListener;
import io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.bedrock.BedrockThinkingConfig.additionalModelRequestFields;
import static org.assertj.core.api.Assertions.assertThat;

class TestAwsBedrockReasoningEffortRequests
{
    @Test
    void testAbsentEffortIsOmittedFromBothRequests()
    {
        CapturedRequests captured = send(Optional.empty());

        assertThat(captured.synchronous().additionalModelRequestFields()).isNull();
        assertThat(captured.streaming().additionalModelRequestFields()).isNull();
    }

    @Test
    void testEffortIsSentOnBothRequests()
    {
        for (ReasoningEffort effort : ReasoningEffort.values()) {
            Optional<Document> expected = additionalModelRequestFields(Optional.of(effort));
            CapturedRequests captured = send(expected);

            assertThat(captured.synchronous().additionalModelRequestFields()).isEqualTo(expected.orElseThrow());
            assertThat(captured.streaming().additionalModelRequestFields()).isEqualTo(expected.orElseThrow());
        }
    }

    private static CapturedRequests send(Optional<Document> additionalModelRequestFields)
    {
        ConverseRequest[] synchronous = new ConverseRequest[1];
        ConverseStreamRequest[] streaming = new ConverseStreamRequest[1];

        BedrockRuntimeClient client = (BedrockRuntimeClient) Proxy.newProxyInstance(
                BedrockRuntimeClient.class.getClassLoader(),
                new Class<?>[] {BedrockRuntimeClient.class},
                (_, method, args) -> {
                    if (method.getName().equals("converse")) {
                        synchronous[0] = buildConverse(args[0]);
                        return ConverseResponse.builder()
                                .output(output -> output.message(message -> message
                                        .role(ConversationRole.ASSISTANT)
                                        .content(content -> content.text("ok"))))
                                .stopReason(StopReason.END_TURN)
                                .build();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        BedrockRuntimeAsyncClient asyncClient = (BedrockRuntimeAsyncClient) Proxy.newProxyInstance(
                BedrockRuntimeAsyncClient.class.getClassLoader(),
                new Class<?>[] {BedrockRuntimeAsyncClient.class},
                (_, method, args) -> {
                    if (method.getName().equals("converseStream")) {
                        streaming[0] = buildConverseStream(args[0]);
                        return CompletableFuture.completedFuture(null);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        LanguageModelClient languageModelClient = new AwsBedrockLanguageModelClient(
                "model",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                additionalModelRequestFields,
                new StaticPromptDao(),
                directExecutor(),
                1,
                client,
                asyncClient,
                true,
                false,
                OpenTelemetry.noop().getTracer("test"),
                false,
                TokenUsageListener.NOOP);

        languageModelClient.generate("hello", TokenUsageContext.EMPTY);
        languageModelClient.generateWithTools(
                "system",
                ImmutableList.of(new LlmMessage(USER, "hello")),
                ImmutableList.of(),
                _ -> {},
                () -> false,
                TokenUsageContext.EMPTY);

        return new CapturedRequests(synchronous[0], streaming[0]);
    }

    @SuppressWarnings("unchecked")
    private static ConverseRequest buildConverse(Object request)
    {
        if (request instanceof ConverseRequest converseRequest) {
            return converseRequest;
        }
        ConverseRequest.Builder builder = ConverseRequest.builder();
        ((Consumer<ConverseRequest.Builder>) request).accept(builder);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static ConverseStreamRequest buildConverseStream(Object request)
    {
        if (request instanceof ConverseStreamRequest converseStreamRequest) {
            return converseStreamRequest;
        }
        ConverseStreamRequest.Builder builder = ConverseStreamRequest.builder();
        ((Consumer<ConverseStreamRequest.Builder>) request).accept(builder);
        return builder.build();
    }

    private record CapturedRequests(ConverseRequest synchronous, ConverseStreamRequest streaming) {}
}
