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

import io.starburst.ai.model.ConnectionInfo;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;
import io.starburst.ai.model.LanguageModelConnectionSpec;

public interface ModelClientFactory<T extends ConnectionInfo>
{
    LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, T connectionInfo, PromptDao promptDao, TokenUsageListener tokenUsageListener);

    EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, T connectionInfo);
}
