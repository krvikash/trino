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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "kind")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LanguageModelConnectionSpec.class, name = "GENERATE"),
        @JsonSubTypes.Type(value = EmbeddingModelConnectionSpec.class, name = "EMBED"),
})
public sealed interface ModelConnectionSpec
        permits LanguageModelConnectionSpec, EmbeddingModelConnectionSpec
{
    /**
     * Returns the id used to identify the model in queries.
     *
     * @return id
     */
    String id();

    /**
     * Returns the model name which is used by the model provider.
     *
     * @return modelName
     */
    String modelName();

    ConnectionInfo connectionInfo();
}
