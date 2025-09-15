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
import io.starburst.ai.model.PromptOverrides;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PromptDaoWithOverrides
        implements PromptDao
{
    private final PromptDao baseProvider;
    private final Optional<PromptOverrides> overrides;

    public PromptDaoWithOverrides(PromptDao baseProvider, Optional<PromptOverrides> overrides)
    {
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
        this.overrides = requireNonNull(overrides, "overrides is null");
    }

    @Override
    public String analyzeSentimentPrompt()
    {
        return overrides.flatMap(PromptOverrides::analyzeSentimentPrompt).orElseGet(baseProvider::analyzeSentimentPrompt);
    }

    @Override
    public Optional<String> analyzeSentimentSystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::analyzeSentimentSystemPrompt).or(baseProvider::analyzeSentimentSystemPrompt);
    }

    @Override
    public String classifyPrompt()
    {
        return overrides.flatMap(PromptOverrides::classifyPrompt).orElseGet(baseProvider::classifyPrompt);
    }

    @Override
    public Optional<String> classifySystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::classifySystemPrompt).or(baseProvider::classifySystemPrompt);
    }

    @Override
    public String fixGrammarPrompt()
    {
        return overrides.flatMap(PromptOverrides::fixGrammarPrompt).orElseGet(baseProvider::fixGrammarPrompt);
    }

    @Override
    public Optional<String> fixGrammarSystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::fixGrammarSystemPrompt).or(baseProvider::fixGrammarSystemPrompt);
    }

    @Override
    public String maskPrompt()
    {
        return overrides.flatMap(PromptOverrides::maskPrompt).orElseGet(baseProvider::maskPrompt);
    }

    @Override
    public Optional<String> maskSystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::maskSystemPrompt).or(baseProvider::maskSystemPrompt);
    }

    @Override
    public String translatePrompt()
    {
        return overrides.flatMap(PromptOverrides::translatePrompt).orElseGet(baseProvider::translatePrompt);
    }

    @Override
    public Optional<String> translateSystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::translateSystemPrompt).or(baseProvider::translateSystemPrompt);
    }

    @Override
    public String summarizePrompt()
    {
        return overrides.flatMap(PromptOverrides::summarizePrompt).orElseGet(baseProvider::summarizePrompt);
    }

    @Override
    public Optional<String> summarizeSystemPrompt()
    {
        return overrides.flatMap(PromptOverrides::summarizeSystemPrompt).or(baseProvider::summarizeSystemPrompt);
    }

    @Override
    public List<String> systemPrompts()
    {
        if (overrides.flatMap(PromptOverrides::systemPrompts).isEmpty() || overrides.flatMap(PromptOverrides::systemPrompts).get().isEmpty()) {
            return baseProvider.systemPrompts();
        }
        // Dynamically add fallback system prompts as implementations may not be static
        return ImmutableList.<String>builder()
                .addAll(baseProvider.systemPrompts())
                .addAll(overrides.flatMap(PromptOverrides::systemPrompts).get())
                .build();
    }
}
