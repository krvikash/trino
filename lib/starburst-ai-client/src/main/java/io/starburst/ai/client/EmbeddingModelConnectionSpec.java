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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record EmbeddingModelConnectionSpec(
        String id,
        String modelName,
        Optional<Integer> dimensions,
        Optional<String> inferenceProfile,
        ConnectionInfo connectionInfo)
        implements ModelConnectionSpec
{
    public EmbeddingModelConnectionSpec
    {
        requireNonNull(id, "id is null");
        requireNonNull(modelName, "modelName is null");
        requireNonNull(dimensions, "dimensions is null");
        requireNonNull(inferenceProfile, "inferenceProfile is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
    }
}
