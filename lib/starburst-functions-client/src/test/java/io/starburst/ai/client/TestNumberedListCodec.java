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

public class TestNumberedListCodec
{
    @Test
    void testDecode()
            throws Exception
    {
        String response = """
                Some junk before
                1. Sentence 1.
                2. Sentence 2.
                3. Sentence 3.
                """;
        PromptCodec codec = new NumberedListCodec();
        List<String> decoded = codec.decode(response, 1);
        assertThat(decoded).containsExactly("Sentence 1.", "Sentence 2.", "Sentence 3.");
    }

    @Test
    void testDecodeFromIndex()
            throws Exception
    {
        String response = """
                8. Sentence 1.
                9. Sentence 2.
                10. Sentence 3.
                """;
        PromptCodec codec = new NumberedListCodec();
        List<String> decoded = codec.decode(response, 8);
        assertThat(decoded).containsExactly("Sentence 1.", "Sentence 2.", "Sentence 3.");
    }
}
