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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface LanguageModelClient
{
    String analyzeSentiment(String text);

    List<String> analyzeSentimentBatch(List<String> texts);

    String classify(String text, List<String> labels);

    List<String> classifyBatch(List<String> texts, List<String> labels);

    String fixGrammar(String text);

    List<String> fixGrammarBatch(List<String> texts);

    String generate(String prompt);

    String generate(String systemPrompt, String prompt);

    // this method is used in SEP
    String generate(List<LlmMessage> messages);

    // this method is used in SEP
    String generate(String systemPrompt, List<LlmMessage> messages);

    /**
     * Generates a response with tool use support.
     * The LLM can choose to use tools, provide a text response, or both.
     *
     * @param systemPrompt System instructions for the LLM
     * @param messages Conversation history
     * @param tools Available tools the LLM can use
     * @return Response containing text and/or tool calls
     */
    ToolUseResponse generateWithTools(String systemPrompt, List<LlmMessage> messages, List<ToolDefinition<?>> tools);

    /**
     * Generates a response with tool use support.
     * The LLM can choose to use tools, provide a text response, or both.
     *
     * @param systemPrompt System instructions for the LLM
     * @param messages Conversation history
     * @param tools Available tools the LLM can use
     * @param output Consumer that receives incremental text chunks as they are streamed from the LLM
     * @param isCancelled Supplier that returns true when the stream should be cancelled early
     * @return Response containing text and/or tool calls
     */
    ToolUseResponse generateWithTools(String systemPrompt, List<LlmMessage> messages, List<ToolDefinition<?>> tools, Consumer<String> output, Supplier<Boolean> isCancelled);

    String mask(String text, List<String> labels);

    List<String> maskBatch(List<String> texts, List<String> labels);

    String translate(String text, String language);

    List<String> translateBatch(List<String> texts, String language);

    String summarize(String text);

    List<String> summarizeBatch(List<String> text);
}
