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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record PromptOverrides(
        Optional<String> analyzeSentimentPrompt,
        Optional<String> analyzeSentimentSystemPrompt,
        Optional<String> classifyPrompt,
        Optional<String> classifySystemPrompt,
        Optional<String> fixGrammarPrompt,
        Optional<String> fixGrammarSystemPrompt,
        Optional<String> maskPrompt,
        Optional<String> maskSystemPrompt,
        Optional<String> translatePrompt,
        Optional<String> translateSystemPrompt,
        Optional<List<String>> systemPrompts)
{
    public PromptOverrides
    {
        requireNonNull(analyzeSentimentPrompt, "analyzeSentimentPrompt is null");
        requireNonNull(analyzeSentimentSystemPrompt, "analyzeSentimentSystemPrompt is null");
        requireNonNull(classifyPrompt, "classifyPrompt is null");
        requireNonNull(classifySystemPrompt, "classifySystemPrompt is null");
        requireNonNull(fixGrammarPrompt, "fixGrammarPrompt is null");
        requireNonNull(fixGrammarSystemPrompt, "fixGrammarSystemPrompt is null");
        requireNonNull(maskPrompt, "maskPrompt is null");
        requireNonNull(maskSystemPrompt, "maskSystemPrompt is null");
        requireNonNull(translatePrompt, "translatePrompt is null");
        requireNonNull(translateSystemPrompt, "translateSystemPrompt is null");
        requireNonNull(systemPrompts, "systemPrompts is null");
    }
}
