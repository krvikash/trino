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

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public record LanguageModelConnectionSpec(
        String id,
        String modelName,
        Optional<Integer> maxTokens,
        Optional<Float> temperature,
        Optional<Float> topP,
        boolean useDeveloperForSystemRole,
        Optional<PromptOverrides> prompts,
        ConnectionInfo connectionInfo)
        implements ModelConnectionSpec
{
    public LanguageModelConnectionSpec
    {
        requireNonNull(id, "id is null");
        requireNonNull(modelName, "modelName is null");
        requireNonNull(maxTokens, "maxTokens is null");
        requireNonNull(temperature, "temperature is null");
        requireNonNull(topP, "topP is null");
        requireNonNull(prompts, "prompts is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        verify(maxTokens.isEmpty() || maxTokens.get() > 0, "if present maxTokens must be greater than 0");
        verify(temperature.isEmpty() || temperature.get() >= 0, "if present temperature must be a positive number");
        verify(topP.isEmpty() || (topP.get() >= 0 && topP.get() <= 1), "if present top_p must be between 0 and 1");
    }
}
