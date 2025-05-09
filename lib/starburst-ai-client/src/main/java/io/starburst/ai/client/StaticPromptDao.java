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

import java.util.List;

public class StaticPromptDao
        implements PromptDao
{
    private static final String ANALYZE_SENTIMENT = """
            Classify the text below into one of the following labels: [positive, negative, neutral, mixed]
            Output only the label.
            =====
            %s
            """;

    private static final String CLASSIFY = """
            Classify the text below into one of the following JSON encoded labels: %s
            Output the label as a JSON string (not a JSON object).
            Output only the label.
            =====
            %s
            """;

    private static final String FIX_GRAMMAR = """
            Fix the grammar in the text below.
            Output only the text.
            =====
            %s
            """;

    private static final String MASK = """
            Mask the values for each of the JSON encoded labels in the text below.
            Labels: %s
            Replace the values with the text "[MASKED]".
            Never replace the labels with "[MASKED]", only replace the values.
            Output only the masked text.
            Do not output anything else.
            =====
            %s
            """;

    private static final String TRANSLATE = """
            Translate the text below to the language specified.
            The language is encoded as a JSON string.
            Output only the translated text.
            Do not return the original text unless it is in the same language as the source.
            Language: %s
            =====
            %s
            """;

    @Override
    public String analyzeSentimentPrompt()
    {
        return ANALYZE_SENTIMENT;
    }

    @Override
    public String classifyPrompt()
    {
        return CLASSIFY;
    }

    @Override
    public String fixGrammarPrompt()
    {
        return FIX_GRAMMAR;
    }

    @Override
    public String maskPrompt()
    {
        return MASK;
    }

    @Override
    public String translatePrompt()
    {
        return TRANSLATE;
    }

    @Override
    public List<String> systemPrompts()
    {
        return ImmutableList.of();
    }
}
