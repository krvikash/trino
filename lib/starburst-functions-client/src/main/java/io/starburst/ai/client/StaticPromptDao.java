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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class StaticPromptDao
        implements PromptDao
{
    private static final String ANALYZE_SENTIMENT = """
            Classify the text below into one of the following labels: [positive, negative, neutral, mixed]
            Output only the label.
            =====
            %s
            """;

    private static final String ANALYZE_SENTIMENT_BATCH = """
            You are a sentiment analysis assistant.
            You will be provided multiple inputs formatted as a numbered list.
            Classify each of them as one of the following: positive, negative, neutral, mixed.
            Return the sentiment of each input without any extraneous text or mark-up. Format the output as a numbered list.

            For example:

            Input:
            1. I love this product
            2. This is terrible
            3. It's okay
            4. There are some good and some bad things about this

            Output:
            1. positive
            2. negative
            3. neutral
            4. mixed

            Here are the inputs to analyze:

            %s
            """;

    private static final String CLASSIFY = """
            Classify the text below into one of the following JSON encoded labels: %s
            Output the label as a JSON string (not a JSON object).
            Output only the label.
            =====
            %s
            """;

    private static final String CLASSIFY_BATCH = """
            You are a useful assistant specializing in text classification.
            You will be provided multiple text inputs formatted as a numbered list.
            Classify each of them into one of the categories identified by the following labels, encoded as a JSON array: %s.
            Return the classification of each input without any extraneous text or mark-up. Format the output as a numbered list.

            For example, if the provided labels are: ["happy", "sad", "neutral"]

            Input:
            1. Life is good
            2. I'm feeling blue
            3. This is the best day ever
            4. Just a normal day

            Output:
            1. happy
            2. sad
            3. happy
            4. neutral

            Here are the inputs to classify:

            %s
            """;

    private static final String FIX_GRAMMAR = """
            Fix the grammar in the text below.
            Output only the text.
            =====
            %s
            """;

    private static final String FIX_GRAMMAR_BATCH = """
            You will be provided a text consisting of multiple paragraphs enclosed by <p_XXX> XML tags, where XXX represents the paragraph number.
            Correct the grammar, spelling, and punctuation for each of these paragraphs. Do not change the meaning or tone.
            Only return the corrected version without explanations. Do not change anything else, including formatting or spacing.

            For example, if the input is:

            <p_1>
            she dont like when I scream.
            they was pretty upset about it.
            </p_1>
            <p_2>
            its not a good idea to goes alone
            </p_2>
            <p_3>
            I like there cat.
            </p_3>

            The output should be:

            <p_1>
            She doesn't like when I scream.
            They were pretty upset about it.
            </p_1>
            <p_2>
            It's not a good idea to go alone.
            </p_2>
            <p_3>
            I like their cat.
            </p_3>

            Here are the paragraphs which need their grammar corrected:

            %s
            """;

    private static final String MASK = """
            Mask the values for each of the JSON encoded labels in the text below.
            Labels: %s
            Replace the values with the text "[MASKED]".
            Never replace the labels with "[MASKED]", only replace the values.
            Output only the masked text.
            Do not output anything else.
            =====
            %s
            """;

    private static final String MASK_BATCH = """
            You are a useful assistant specializing in masking sensitive fields in text.
            You are provided a text consisting of multiple fragments enclosed by <p_XXX> XML tags, where XXX represents the fragment number.
            You are also provided a list of labels identifying the types of sensitive information that needs to be masked.
            For each label, detect values corresponding to the label in the provided text and replace them with "[MASKED]".
            If none of the sensitive types is present in a fragment, leave the fragment unchanged.
            Do not provide explanations or instructions on how to identify sensitive values, just perform the masking.
            IMPORTANT: Never replace the labels found in the text, just the values corresponding to them.
            For example, if you are asked to mask the address, do not mask the word "address", just the actual address value, like 123 Main St, Anytown, NY 12345.
            Do not alter any other part of the text.

            For example, if the sensitive information types are ["email", "ssn", "address", "phone"] and the input is:

            <p_1>
            I don't have an email address.
            </p_1>
            <p_2>
            Here are the documents from jane.doe@example.com. Her SSN is 123-45-6789 and the office address is 456 Oak Street, Springfield.
            </p_2>
            <p_3>
            Contact me at 987-615-4320 or at my work email.
            </p_3>

            The output should be (notice how only the sensitive values are masked, not the labels, like "email" or "address"):

            <p_1>
            I don't have an email address.
            </p_1>
            <p_2>
            Here are the documents from [MASKED]. Her SSN is [MASKED] and the office address is [MASKED].
            </p_2>
            <p_3>
            Contact me at [MASKED] or at my work email.
            </p_3>

            The sensitive types of information that need to be masked are: %s

            Mask the corresponding sensitive values in the following text:

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

    private static final String TRANSLATE_BATCH = """
            You are a useful assistant specializing in translating text into various languages.
            You are provided a text consisting of multiple fragments enclosed by <p_XXX> XML tags, where XXX represents the fragment number.
            You are also provided the language to which the text should be translated.
            Replace the text between each pair of XML tags with the translated text.
            Do not return the original text unless it is already in the target language.
            Do not return any explanations or instructions, just the translated text.

            For example, if instructed to translate to German and the input is:

            <p_1>
            I received your letter yesterday. Thank you very much for the information.
            </p_1>
            <p_2>
            I arrived in Berlin yesterday afternoon. The city is full of life and history, and the people have been very welcoming.
            Today, I visited the Brandenburg Gate and took a walk through the Tiergarten. The weather was perfect — sunny and cool.
            I'm looking forward to exploring more tomorrow.
            </p_2>

            The expected output would be:

            <p_1>
            Ich habe gestern deinen Brief erhalten. Vielen Dank für die Informationen.
            </p_1>
            <p_2>
            Ich bin gestern Nachmittag in Berlin angekommen. Die Stadt ist voller Leben und Geschichte, und die Menschen waren sehr freundlich.
            Heute habe ich das Brandenburger Tor besucht und einen Spaziergang durch den Tiergarten gemacht. Das Wetter war perfekt – sonnig und kühl.
            Ich freue mich darauf, morgen noch mehr zu entdecken.
            </p_2>

            Translate the text below into the following language: %s

            %s
            """;

    private static final String SUMMARIZE_SYSTEM_PROMPT = """
            You are a summarization AI assistant.
            The user will only prompt with text that they want summarized.
            Do not respond with anything outside of the text: no opinions or insights.
            Summarize the user prompt which will consist of text that they want you to summarize.
            Be concise and limit to several bullet points.
            The length of the output should be significantly shorter than the input:
            If the input is a few sentences or a paragraph then output should be 2 sentences maximum no exceptions.
            If the input is a few paragraphs then output a single 3 sentence paragraph.
            For input longer than a few paragraphs, output at most a single 5 sentence paragraph.
            """;

    private static final String SUMMARIZE_BATCH = """
            The text to be summarized consists of multiple fragments enclosed by <p_XXX> XML tags, where XXX represents the fragment number.
            Replace each fragment by its summary and return it. Preserve the corresponding <p_XXX> XML tags.

            %s
            """;

    @Override
    public String analyzeSentimentPrompt()
    {
        return ANALYZE_SENTIMENT;
    }

    @Override
    public String analyzeSentimentPromptBatch()
    {
        return ANALYZE_SENTIMENT_BATCH;
    }

    @Override
    public String classifyPrompt()
    {
        return CLASSIFY;
    }

    @Override
    public String classifyPromptBatch()
    {
        return CLASSIFY_BATCH;
    }

    @Override
    public String fixGrammarPrompt()
    {
        return FIX_GRAMMAR;
    }

    @Override
    public String fixGrammarPromptBatch()
    {
        return FIX_GRAMMAR_BATCH;
    }

    @Override
    public String maskPrompt()
    {
        return MASK;
    }

    @Override
    public String maskPromptBatch()
    {
        return MASK_BATCH;
    }

    @Override
    public String translatePrompt()
    {
        return TRANSLATE;
    }

    @Override
    public String translatePromptBatch()
    {
        return TRANSLATE_BATCH;
    }

    @Override
    public String summarizePrompt()
    {
        return "%s";
    }

    @Override
    public String summarizePromptBatch()
    {
        return SUMMARIZE_BATCH;
    }

    @Override
    public Optional<String> summarizeSystemPrompt()
    {
        return Optional.of(SUMMARIZE_SYSTEM_PROMPT);
    }

    @Override
    public List<String> systemPrompts()
    {
        return ImmutableList.of();
    }
}
