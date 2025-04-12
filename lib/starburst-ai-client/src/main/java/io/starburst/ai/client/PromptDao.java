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

    /**
     * Set guidelines on tone, formality, restrict use of offensive language, etc.
     * Note that this is called developer prompt by some providers.
     *
     * @return Layered system prompt
     */
    List<String> systemPrompts();
}
