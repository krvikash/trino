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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.openai.helpers.ChatCompletionAccumulator;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionChunk;
import com.openai.models.completions.CompletionUsage;
import io.starburst.ai.client.openai.OpenAiLanguageModelClient.AccumulatableChunkFilter;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.openai.core.ObjectMappers.jsonMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAccumulatableChunkFilter
{
    private static final ChatCompletionChunk CONTENT = chunk(
            """
            {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4.1-mini", "choices": [{"index": 0, "finish_reason": null, "delta": {"role": "assistant", "content": "62"}}], "usage": null}
            """);
    private static final ChatCompletionChunk FINISH = chunk(
            """
            {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4.1-mini", "choices": [{"index": 0, "finish_reason": "stop", "delta": {}}], "usage": null}
            """);
    private static final ChatCompletionChunk USAGE = chunk(
            """
            {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4.1-mini", "choices": [], "usage": {"prompt_tokens": 20, "completion_tokens": 2, "total_tokens": 22}}
            """);
    // Azure OpenAI content filter annotation, see https://learn.microsoft.com/en-us/azure/ai-foundry/openai/concepts/content-streaming
    private static final ChatCompletionChunk ANNOTATION = chunk(
            """
            {"id": "", "object": "", "created": 0, "model": "", "choices": [{"index": 0, "finish_reason": null, "content_filter_results": {"hate": {"filtered": false, "severity": "safe"}}, "content_filter_offsets": {"check_offset": 2, "start_offset": 0, "end_offset": 2}}], "usage": null}
            """);

    @Test
    void testAccumulatorRejectsAnnotationAfterFinishReason()
    {
        assertThatThrownBy(() -> accumulate(ImmutableList.of(CONTENT, FINISH, ANNOTATION, USAGE)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Already accumulated the final chunk(s).");
    }

    @Test
    void testKeepsAllChunksOfCleanStream()
    {
        List<ChatCompletionChunk> chunks = filter(ImmutableList.of(CONTENT, FINISH, USAGE));

        assertThat(chunks).containsExactly(CONTENT, FINISH, USAGE);
        assertCompletion(accumulate(chunks), Optional.of(22L));
    }

    @Test
    void testDropsAnnotationsBeforeUsage()
    {
        List<ChatCompletionChunk> chunks = filter(ImmutableList.of(CONTENT, FINISH, ANNOTATION, ANNOTATION, USAGE));

        assertThat(chunks).containsExactly(CONTENT, FINISH, USAGE);
        assertCompletion(accumulate(chunks), Optional.of(22L));
    }

    @Test
    void testDropsAnnotationsAfterUsage()
    {
        List<ChatCompletionChunk> chunks = filter(ImmutableList.of(CONTENT, FINISH, USAGE, ANNOTATION));

        assertThat(chunks).containsExactly(CONTENT, FINISH, USAGE);
        assertCompletion(accumulate(chunks), Optional.of(22L));
    }

    @Test
    void testDropsAnnotationsWithoutUsage()
    {
        List<ChatCompletionChunk> chunks = filter(ImmutableList.of(CONTENT, FINISH, ANNOTATION));

        assertThat(chunks).containsExactly(CONTENT, FINISH);
        assertCompletion(accumulate(chunks), Optional.empty());
    }

    @Test
    void testKeepsOnlyFirstUsageChunk()
    {
        List<ChatCompletionChunk> chunks = filter(ImmutableList.of(CONTENT, FINISH, USAGE, USAGE));

        assertThat(chunks).containsExactly(CONTENT, FINISH, USAGE);
        assertCompletion(accumulate(chunks), Optional.of(22L));
    }

    private static void assertCompletion(ChatCompletion completion, Optional<Long> expectedTotalTokens)
    {
        assertThat(completion.choices()).hasSize(1);
        assertThat(completion.choices().getFirst().finishReason()).isEqualTo(ChatCompletion.Choice.FinishReason.STOP);
        assertThat(completion.choices().getFirst().message().content()).contains("62");
        assertThat(completion.usage().map(CompletionUsage::totalTokens)).isEqualTo(expectedTotalTokens);
    }

    private static List<ChatCompletionChunk> filter(List<ChatCompletionChunk> chunks)
    {
        return chunks.stream()
                .filter(new AccumulatableChunkFilter())
                .collect(toImmutableList());
    }

    private static ChatCompletion accumulate(List<ChatCompletionChunk> chunks)
    {
        ChatCompletionAccumulator accumulator = ChatCompletionAccumulator.create();
        chunks.forEach(accumulator::accumulate);
        return accumulator.chatCompletion();
    }

    private static ChatCompletionChunk chunk(String json)
    {
        try {
            return jsonMapper().readValue(json, ChatCompletionChunk.class);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
