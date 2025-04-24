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

import io.airlift.slice.Slice;

import java.util.List;

public interface EmbeddingModelClient
{
    List<Double> generateEmbedding(Slice sourceString);

    List<List<Float>> generateEmbeddings(List<Slice> sourceStrings);

    default Slice generateBinaryEmbedding(Slice sourceString)
    {
        throw new UnsupportedOperationException("Binary embeddings are not supported");
    }

    default List<Slice> generateBinaryEmbeddings(List<Slice> sourceString)
    {
        throw new UnsupportedOperationException("Binary embeddings are not supported");
    }
}
