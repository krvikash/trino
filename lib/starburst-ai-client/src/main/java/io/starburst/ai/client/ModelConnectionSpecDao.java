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

import java.util.Collection;

public interface ModelConnectionSpecDao
{
    Collection<LanguageModelConnectionSpec> languageModelConnectionSpecs();

    Collection<EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs();

    LanguageModelConnectionSpec getLanguageModelConnectionSpecById(String id);

    EmbeddingModelConnectionSpec getEmbeddingModelConnectionSpecById(String id);
}
