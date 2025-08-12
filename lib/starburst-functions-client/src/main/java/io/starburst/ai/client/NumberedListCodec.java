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

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class NumberedListCodec
        implements PromptCodec
{
    @Override
    public String encode(String input, int fragmentNumber)
    {
        checkArgument(!Strings.isNullOrEmpty(input), "input cannot be empty");
        checkArgument(fragmentNumber > 0, "fragmentNumber must be positive");
        return fragmentNumber + ". " + input.replace("\n", " ").replace("\r", " ") + "\n";
    }

    @Override
    public List<String> decode(String response, int fragmentNumber)
            throws LlmResponseException
    {
        checkArgument(!Strings.isNullOrEmpty(response), "response cannot be empty");
        ImmutableList.Builder<String> outputs = ImmutableList.builder();
        Iterator<String> lines = response.lines().iterator();
        int currentLine = fragmentNumber;
        while (lines.hasNext()) {
            String line = lines.next();
            // skip any preamble the LLM might have included
            if (currentLine == fragmentNumber && !line.startsWith(currentLine + ".")) {
                continue;
            }
            if (!line.startsWith(currentLine + ".")) {
                throw new LlmResponseException("Malformed response: expected line to start with '" + currentLine + ". ', but got: " + line);
            }
            outputs.add(line.substring(line.indexOf('.') + 1).trim());
            currentLine++;
        }

        return outputs.build();
    }
}
