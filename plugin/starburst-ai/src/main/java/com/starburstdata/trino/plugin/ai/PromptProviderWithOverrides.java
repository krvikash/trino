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
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PromptProviderWithOverrides
        implements PromptProvider
{
    private final PromptProvider baseProvider;
    private final Optional<Prompts> overrides;

    public PromptProviderWithOverrides(PromptProvider baseProvider, Optional<Prompts> overrides)
    {
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
        this.overrides = requireNonNull(overrides, "overrides is null");
    }

    @Override
    public String analyzeSentiment()
    {
        return overrides.flatMap(Prompts::analyzeSentimentPrompt).orElseGet(baseProvider::analyzeSentiment);
    }

    @Override
    public Optional<String> analyzeSentimentSystem()
    {
        return overrides.flatMap(Prompts::analyzeSentimentSystemPrompt).or(baseProvider::analyzeSentimentSystem);
    }

    @Override
    public String classify()
    {
        return overrides.flatMap(Prompts::classifyPrompt).orElseGet(baseProvider::classify);
    }

    @Override
    public Optional<String> classifySystem()
    {
        return overrides.flatMap(Prompts::classifySystemPrompt).or(baseProvider::classifySystem);
    }

    @Override
    public String fixGrammar()
    {
        return overrides.flatMap(Prompts::fixGrammarPrompt).orElseGet(baseProvider::fixGrammar);
    }

    @Override
    public Optional<String> fixGrammarSystem()
    {
        return overrides.flatMap(Prompts::fixGrammarSystemPrompt).or(baseProvider::fixGrammarSystem);
    }

    @Override
    public String mask()
    {
        return overrides.flatMap(Prompts::maskPrompt).orElseGet(baseProvider::mask);
    }

    @Override
    public Optional<String> maskSystem()
    {
        return overrides.flatMap(Prompts::maskSystemPrompt).or(baseProvider::maskSystem);
    }

    @Override
    public String translate()
    {
        return overrides.flatMap(Prompts::translatePrompt).orElseGet(baseProvider::translate);
    }

    @Override
    public Optional<String> translateSystem()
    {
        return overrides.flatMap(Prompts::translateSystemPrompt).or(baseProvider::translateSystem);
    }

    @Override
    public List<String> system()
    {
        if (overrides.flatMap(Prompts::systemPrompts).isEmpty() || overrides.flatMap(Prompts::systemPrompts).get().isEmpty()) {
            return baseProvider.system();
        }
        // Dynamically add fallback system prompts as implementations may not be static
        return ImmutableList.<String>builder()
                .addAll(baseProvider.system())
                .addAll(overrides.flatMap(Prompts::systemPrompts).get())
                .build();
    }
}
