/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class StaticPromptProvider
        implements PromptProvider
{
    private static final String ANALYZE_SENTIMENT = """
            Classify the text below into one of the following labels: [positive, negative, neutral, mixed]
            Output only the label.
            =====
            %s
            """;

    private static final String CLASSIFY = """
            Classify the text below into one of the following JSON encoded labels: %s
            Output the label as a JSON string (not a JSON object).
            Output only the label.
            =====
            %s
            """;

    private static final String FIX_GRAMMAR = """
            Fix the grammar in the text below.
            Output only the text.
            =====
            %s
            """;

    private static final String MASK = """
            Mask the values for each of the JSON encoded labels in the text below.
            Labels: %s
            Replace the values with the text "[MASKED]".
            Output only the masked text.
            Do not output anything else.
            =====
            %s
            """;

    private static final String TRANSLATE = """
            Translate the text below to the language specified.
            The language is encoded as a JSON string.
            Output only the translated text.
            Do not return the original text unless it is in the same language as the source.
            Language: %s
            =====
            %s
            """;

    @Override
    public String analyzeSentiment()
    {
        return ANALYZE_SENTIMENT;
    }

    @Override
    public String classify()
    {
        return CLASSIFY;
    }

    @Override
    public String fixGrammar()
    {
        return FIX_GRAMMAR;
    }

    @Override
    public String mask()
    {
        return MASK;
    }

    @Override
    public String translate()
    {
        return TRANSLATE;
    }

    @Override
    public List<String> system()
    {
        return ImmutableList.of();
    }
}
