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
