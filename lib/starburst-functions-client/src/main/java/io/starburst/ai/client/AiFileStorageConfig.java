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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotEmpty;

public class AiFileStorageConfig
{
    private String modelConnectionSpecsFile;

    @NotEmpty
    public String getModelConnectionSpecsFile()
    {
        return modelConnectionSpecsFile;
    }

    @Config("ai.client.models.file")
    @ConfigDescription("Path to the file containing the model connection specs configuration")
    public AiFileStorageConfig setModelConnectionSpecsFile(String modelConnectionSpecsFile)
    {
        this.modelConnectionSpecsFile = modelConnectionSpecsFile;
        return this;
    }
}
