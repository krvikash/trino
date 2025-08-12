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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Codec that wraps each prompt in XML-like tags, e.g.:
 * &lt;p_1&gt;
 * prompt text
 * &lt;/p_1&gt;
 * &lt;p_2&gt;
 * prompt text
 * &lt;/p_2&gt;
 * ...
 * This is useful for LLM batch responses where items could span multiple lines. The tags are mere separators and use
 * XML syntax because simply because they work well with LLMs. These are not going to be well-formed XML documents.
 */
public class XmlTagsCodec
        implements PromptCodec
{
    @Override
    public String encode(String input, int fragmentNumber)
    {
        checkArgument(input != null, "input cannot be null");
        checkArgument(fragmentNumber >= 0, "fragmentNumber must be >= 0");
        return startTag(fragmentNumber) + "\n" + input.replace(endTag(fragmentNumber), escapedEndTag(fragmentNumber)) + "\n" + endTag(fragmentNumber) + "\n";
    }

    @Override
    public List<String> decode(String response, int fragmentNumber)
            throws LlmResponseException
    {
        checkArgument(!Strings.isNullOrEmpty(response), "response cannot be empty");
        ImmutableList.Builder<String> outputs = ImmutableList.builder();

        int startIndex = 0;
        while (startIndex < response.length()) {
            String startTag = startTag(fragmentNumber);
            int start = response.indexOf(startTag, startIndex);
            if (start == -1) {
                break;
            }
            int contentStart = start + startTag.length();

            String endTag = endTag(fragmentNumber);
            int end = response.indexOf(endTag, contentStart);
            if (end == -1) {
                throw new LlmResponseException("Malformed response: missing end tag for paragraph " + fragmentNumber);
            }

            String content = response.substring(contentStart, end).trim().replace(escapedEndTag(fragmentNumber), endTag);
            outputs.add(content);

            startIndex = end + endTag.length();
            fragmentNumber++;
        }

        return outputs.build();
    }

    private static String startTag(int index)
    {
        return "<p_%s>".formatted(index);
    }

    private static String endTag(int index)
    {
        return "</p_%s>".formatted(index);
    }

    private static String escapedEndTag(int index)
    {
        return "&lt;/p_%s&gt;".formatted(index);
    }
}
