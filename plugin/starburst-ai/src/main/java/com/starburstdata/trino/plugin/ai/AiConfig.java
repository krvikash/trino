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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotEmpty;

public class AiConfig
{
    private String modelConnectionSpecsFile;

    @NotEmpty
    public String getModelConnectionSpecsFile()
    {
        return modelConnectionSpecsFile;
    }

    @Config("ai.models-file")
    @ConfigDescription("Path to the file containing the model connection specs configuration")
    public AiConfig setModelConnectionSpecsFile(String modelConnectionSpecsFile)
    {
        this.modelConnectionSpecsFile = modelConnectionSpecsFile;
        return this;
    }
}
