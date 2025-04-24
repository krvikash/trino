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

import java.util.List;

public interface LanguageModelClient
{
    String analyzeSentiment(String text);

    String classify(String text, List<String> labels);

    String fixGrammar(String text);

    String generate(String prompt);

    String generate(String systemPrompt, String prompt);

    String mask(String text, List<String> labels);

    String translate(String text, String language);
}
