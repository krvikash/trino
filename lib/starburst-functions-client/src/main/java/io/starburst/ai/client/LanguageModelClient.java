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

    String mask(String text, List<String> labels);

    List<String> maskBatch(List<String> texts, List<String> labels);

    String translate(String text, String language);

    List<String> translateBatch(List<String> texts, String language);

    String summarize(String text);

    List<String> summarizeBatch(List<String> text);
}
