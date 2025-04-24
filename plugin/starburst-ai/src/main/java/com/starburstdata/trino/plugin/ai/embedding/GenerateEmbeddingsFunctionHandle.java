/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai.embedding;

import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingType;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import static java.util.Objects.requireNonNull;

public record GenerateEmbeddingsFunctionHandle(Slice modelId, EmbeddingType embeddingType)
        implements ConnectorTableFunctionHandle
{
    public GenerateEmbeddingsFunctionHandle
    {
        requireNonNull(modelId, "modelId is null");
        requireNonNull(embeddingType, "embeddingType is null");
    }
}
