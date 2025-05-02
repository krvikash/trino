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

public interface PromptDao
{
    String analyzeSentimentPrompt();

    default Optional<String> analyzeSentimentSystemPrompt()
    {
        return Optional.empty();
    }

    String classifyPrompt();

    default Optional<String> classifySystemPrompt()
    {
        return Optional.empty();
    }

    String fixGrammarPrompt();

    default Optional<String> fixGrammarSystemPrompt()
    {
        return Optional.empty();
    }

    String maskPrompt();

    default Optional<String> maskSystemPrompt()
    {
        return Optional.empty();
    }

    String translatePrompt();

    default Optional<String> translateSystemPrompt()
    {
        return Optional.empty();
    }

    String summarizePrompt();

    default Optional<String> summarizeSystemPrompt()
    {
        return Optional.empty();
    }

    /**
     * Set guidelines on tone, formality, restrict use of offensive language, etc.
     * Note that this is called developer prompt by some providers.
     *
     * @return Layered system prompt
     */
    List<String> systemPrompts();
}
