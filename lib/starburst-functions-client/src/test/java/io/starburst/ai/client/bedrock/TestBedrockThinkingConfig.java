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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.document.Document;

import java.util.Optional;

import static io.starburst.ai.client.bedrock.BedrockThinkingConfig.additionalModelRequestFields;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.none;
import static org.assertj.core.api.Assertions.assertThat;

class TestBedrockThinkingConfig
{
    @Test
    void testNoEffortSendsNothing()
    {
        assertThat(additionalModelRequestFields(Optional.empty())).isEmpty();
    }

    @Test
    void testNoneDisablesThinking()
    {
        assertThat(additionalModelRequestFields(Optional.of(none)))
                .contains(Document.mapBuilder()
                        .putDocument("thinking", Document.mapBuilder().putString("type", "disabled").build())
                        .build());
    }

    @Test
    void testEffortEnablesAdaptiveThinking()
    {
        for (ReasoningEffort effort : ReasoningEffort.values()) {
            if (effort == none) {
                continue;
            }
            assertThat(additionalModelRequestFields(Optional.of(effort)))
                    .contains(Document.mapBuilder()
                            .putDocument("thinking", Document.mapBuilder().putString("type", "adaptive").build())
                            .putDocument("output_config", Document.mapBuilder().putString("effort", effort.name()).build())
                            .build());
        }
    }
}
