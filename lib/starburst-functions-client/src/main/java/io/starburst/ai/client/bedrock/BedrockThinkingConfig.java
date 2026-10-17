/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort;
import software.amazon.awssdk.core.document.Document;

import java.util.Optional;

final class BedrockThinkingConfig
{
    private BedrockThinkingConfig() {}

    // Bedrock rejects efforts the model does not support
    static Optional<Document> additionalModelRequestFields(Optional<ReasoningEffort> reasoningEffort)
    {
        return reasoningEffort.map(effort -> switch (effort) {
            case none -> Document.mapBuilder()
                    .putDocument("thinking", Document.mapBuilder().putString("type", "disabled").build())
                    .build();
            case minimal, low, medium, high, xhigh, max -> Document.mapBuilder()
                    .putDocument("thinking", Document.mapBuilder().putString("type", "adaptive").build())
                    .putDocument("output_config", Document.mapBuilder().putString("effort", effort.name()).build())
                    .build();
        });
    }
}
