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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestXmlTagsCodec
{
    @Test
    void testDecode()
            throws Exception
    {
        String response =
                """
                Some junk before
                <p_1>
                Sentence 1.
                Sentence 2.
                </p_1>
                <p_2>
                Sentence 3.
                &lt;/p_2&gt;
                </p_2>
                <p_3>Sentence 4.</p_3><p_4>Sentence 5.</p_4>
                """;
        PromptCodec codec = new XmlTagsCodec();
        List<String> decoded = codec.decode(response, 1);
        assertThat(decoded).containsExactly("Sentence 1.\nSentence 2.", "Sentence 3.\n</p_2>", "Sentence 4.", "Sentence 5.");
    }

    @Test
    void testDecodeFromIndex()
            throws Exception
    {
        String response =
                """
                <p_8>
                Sentence 1.
                Sentence 2.
                </p_8>
                <p_9>
                Sentence 3.
                </p_9>
                <p_10>Sentence 4.</p_10><p_11>Sentence 5.</p_11>
                """;
        PromptCodec codec = new XmlTagsCodec();
        List<String> decoded = codec.decode(response, 8);
        assertThat(decoded).containsExactly("Sentence 1.\nSentence 2.", "Sentence 3.", "Sentence 4.", "Sentence 5.");
    }
}
