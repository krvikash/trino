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
package io.starburst.ai.client.bedrock;

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.AbstractLanguageModelClient;
import io.starburst.ai.client.PromptDao;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseResponse;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.StopReason;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_RESPONSE_MODEL;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_SYSTEM;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_INPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GEN_AI_USAGE_OUTPUT_TOKENS;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiOperationNameIncubatingValues.CHAT;
import static io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes.GenAiSystemIncubatingValues.AWS_BEDROCK;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class AwsBedrockLanguageModelClient
        extends AbstractLanguageModelClient
{
    private static final Set<StopReason> ERROR_STOP_REASONS = ImmutableSet.of(
            StopReason.UNKNOWN_TO_SDK_VERSION,
            StopReason.GUARDRAIL_INTERVENED,
            StopReason.CONTENT_FILTERED,
            StopReason.TOOL_USE,
            StopReason.MAX_TOKENS);

    private final Optional<Integer> maxTokens;
    private final Optional<Float> temperature;
    private final Optional<Float> topP;
    private final Tracer tracer;
    private final BedrockRuntimeClient client;

    public AwsBedrockLanguageModelClient(
            String modelName,
            Optional<Integer> maxTokens,
            Optional<Float> temperature,
            Optional<Float> topP,
            PromptDao promptDao,
            Tracer tracer,
            BedrockRuntimeClient client)
    {
        super(modelName, promptDao);
        this.maxTokens = requireNonNull(maxTokens, "maxTokens is null");
        this.temperature = requireNonNull(temperature, "temperature is null");
        this.topP = requireNonNull(topP, "topP is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    protected String generateCompletion(String model, List<String> systemPrompts, String prompt)
    {
        List<SystemContentBlock> systemContentBlocks = systemPrompts.stream()
                .map(SystemContentBlock::fromText)
                .collect(toImmutableList());

        Span span = tracer.spanBuilder(CHAT + " " + model)
                .setAttribute(GEN_AI_OPERATION_NAME, CHAT)
                .setAttribute(GEN_AI_SYSTEM, AWS_BEDROCK)
                .setAttribute(GEN_AI_REQUEST_MODEL, model)
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        ConverseResponse response;
        try (var _ = span.makeCurrent()) {
            response = client.converse(request -> {
                if (!systemContentBlocks.isEmpty()) {
                    request.system(systemContentBlocks);
                }
                request
                        .modelId(model)
                        .messages(Message.builder()
                                .role(ConversationRole.USER)
                                .content(ContentBlock.fromText(prompt))
                                .build())
                        .inferenceConfig(config -> config
                                .maxTokens(maxTokens.orElse(null))
                                .temperature(temperature.orElse(null))
                                .topP(topP.orElse(null)));
            });

            span.setAttribute(GEN_AI_RESPONSE_MODEL, model);
            span.setAttribute(GEN_AI_USAGE_INPUT_TOKENS, Optional.ofNullable(response.usage().inputTokens()).orElse(0));
            span.setAttribute(GEN_AI_USAGE_OUTPUT_TOKENS, Optional.ofNullable(response.usage().outputTokens()).orElse(0));
        }
        catch (RuntimeException e) {
            span.setStatus(ERROR, e.getMessage());
            span.recordException(e);
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to execute AI request", e);
        }
        finally {
            span.end();
        }
        List<ContentBlock> contentBlocks = response.output().message().content();
        if (response.stopReason() != null && ERROR_STOP_REASONS.contains(response.stopReason())) {
            throw new TrinoException(AI_CLIENT_ERROR, "AI model refused to generate response: " + response.stopReasonAsString());
        }
        if (contentBlocks == null || contentBlocks.isEmpty() || response.output().message().content().getFirst().text() == null) {
            return "";
        }

        return response.output().message().content().getFirst().text();
    }
}
