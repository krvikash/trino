/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static java.util.Locale.ROOT;

/**
 * Utility class for parsing Link headers based on RFC-8288.
 */
public final class LinkHeaderUtil
{
    // OWS = *( SP / HTAB ) - RFC7230.
    private static final CharMatcher OPTIONAL_WHITE_SPACE = CharMatcher.anyOf(" \t");
    // BWS = OWS - RFC7230
    private static final CharMatcher BAD_WHITE_SPACE = OPTIONAL_WHITE_SPACE;

    private LinkHeaderUtil() {}

    /**
     * Unoptimized java reference implementation of <a href="https://datatracker.ietf.org/doc/html/rfc8288#appendix-B.2">RFC-8288 appendix B.2 pseudocode</a>.
     */
    public static List<LinkFieldValue> parseLinkFieldValue(String string)
    {
        // 1. Let links be an empty list.
        ImmutableList.Builder<LinkFieldValue> listBuilder = ImmutableList.builder();
        // 2. While field_value has content:
        int index = 0;
        while (index < string.length()) {
            // 1. Consume any leading OWS.
            while (index < string.length() && OPTIONAL_WHITE_SPACE.matches(string.charAt(index))) {
                index++;
            }
            // 2. If the first character is not "<", return links.
            if (index == string.length() || string.charAt(index) != '<') {
                return listBuilder.build();
            }
            // 3. Discard the first character ("<").
            index++;
            // 4. Consume up to but not including the first ">" character or
            // end of field_value and let the result be target_string.
            int endTargetStringIndex = string.indexOf('>', index);
            if (endTargetStringIndex == -1) {
                // 5. If the next character is not ">", return links.
                return listBuilder.build();
            }
            String targetString = string.substring(index, endTargetStringIndex);
            index = endTargetStringIndex;
            // 6. Discard the leading ">" character.
            index++;
            // 7. Let link_parameters be the result of Parsing Parameters (Appendix B.3)
            // from field_value (consuming zero or more characters of it).
            ParameterParseResult parseResult = parseParameters(index, string);
            index = parseResult.index();
            Map<String, String> linkParameters = parseResult.parameters();
            // We skip step 8, we can relatively resolve URIs later.
            URI targetUri = URI.create(targetString);
            listBuilder.add(new LinkFieldValue(targetUri, linkParameters));
            // Skipping the rest of reference implementation as it has more to do with categorization.
        }
        return listBuilder.build();
    }

    /**
     * Unoptimized java reference implementation of <a href="https://datatracker.ietf.org/doc/html/rfc8288#appendix-B.3">RFC-8288 appendix B.3 pseudocode</a>.
     */
    private static ParameterParseResult parseParameters(int index, String string)
    {
        // 1. Let parameters be an empty list.
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        // 2. While input has content:
        while (index < string.length()) {
            // 1. Consume any leading OWS.
            while (index < string.length() && OPTIONAL_WHITE_SPACE.matches(string.charAt(index))) {
                index++;
            }
            // 2. If the first character is not ";", return parameters.
            if (index == string.length() || string.charAt(index) != ';') {
                break;
            }
            // 3. Discard the leading ";" character.
            index++;
            // 4. Consume any leading OWS.
            while (index < string.length() && OPTIONAL_WHITE_SPACE.matches(string.charAt(index))) {
                index++;
            }
            // 5. Consume up to but not including the first BWS, "=", ";",
            // or "," character,
            // or up to the end of input,
            // and let the result be parameter_name.
            int startParameterNameIndex = index;
            while (index < string.length()) {
                char currentCharacter = string.charAt(index);
                if (BAD_WHITE_SPACE.matches(currentCharacter) ||
                        currentCharacter == '=' ||
                        currentCharacter == ';' ||
                        currentCharacter == ',') {
                    break;
                }
                index++;
            }
            String parameterName = string.substring(startParameterNameIndex, index);
            String parameterValue;
            // 6. Consume any leading BWS.
            while (index < string.length() && BAD_WHITE_SPACE.matches(string.charAt(index))) {
                index++;
            }
            // 7. If the next character is "=":
            if (index < string.length() && string.charAt(index) == '=') {
                // 1. Discard the leading "=" character.
                index++;
                // 2. Consume any leading BWS.
                while (index < string.length() && BAD_WHITE_SPACE.matches(string.charAt(index))) {
                    index++;
                }
                // 3. If the next character is DQUOTE,
                // let parameter_value be the result of Parsing a Quoted String (Appendix B.4)
                // from input (consuming zero or more characters of it).
                if (index < string.length() && string.charAt(index) == '"') {
                    QuotedParseResult parseResult = parseQuoted(index, string);
                    index = parseResult.index();
                    parameterValue = parseResult.quoted();
                }
                // 4. Else,
                // consume the contents up to but not including the first ";" or "," character,
                // or up to the end of input,
                // and let the results be parameter_value.
                else {
                    int startParameterValueIndex = index;
                    while (index < string.length()) {
                        char currentCharacter = string.charAt(index);
                        if (currentCharacter == ';' || currentCharacter == ',') {
                            break;
                        }
                        index++;
                    }
                    parameterValue = string.substring(startParameterValueIndex, index);
                }
                // 5.  If the last character of parameter_name is an asterisk ("*"),
                // decode parameter_value according to [RFC8187].
                // Continue processing input if an unrecoverable error is encountered.
                if (!parameterName.isEmpty() && parameterName.charAt(parameterName.length() - 1) == '*') {
                    throw new UnsupportedOperationException("Unsupported RFC8187 encoded field parameter.");
                }
            }
            // 8.1. Else: Let parameter_value be an empty string.
            else {
                parameterValue = "";
            }
            // 9. Case-normalise parameter_name to lowercase.
            // 10. Append (parameter_name, parameter_value) to parameters.
            mapBuilder.put(parameterName.toLowerCase(ROOT), parameterValue);
            // 11. Consume any leading OWS.
            while (index < string.length() && OPTIONAL_WHITE_SPACE.matches(string.charAt(index))) {
                index++;
            }
            // 12. If the next character is "," or the end of input,
            // stop processing input and return parameters.
            if (index < string.length() && string.charAt(index) == ',') {
                // parseLinkFieldValue depends on discarding this comma.
                index++;
            }
        }
        return new ParameterParseResult(index, mapBuilder.buildOrThrow());
    }

    /**
     * Unoptimized java reference implementation of <a href="https://datatracker.ietf.org/doc/html/rfc8288#appendix-B.4">RFC-8288 appendix B.4 pseudocode</a>.
     */
    private static QuotedParseResult parseQuoted(int index, String string)
    {
        // 1. Let output be an empty string.
        StringBuilder stringBuilder = new StringBuilder();
        // 2. If the first character of input is not DQUOTE, return output.
        if (string.charAt(index) != '"') {
            return new QuotedParseResult(index, stringBuilder.toString());
        }
        // 3. Discard the first character.
        index++;
        // 4. While input has content:
        while (index < string.length()) {
            // 1. If the first character is a backslash ("\"):
            char character = string.charAt(index);
            if (character == '\\') {
                // 1. Discard the first character.
                index++;
                // 2. If there is no more input, return output.
                if (index == string.length()) {
                    break;
                }
                // 3. Else, consume the first character and append it to output.
                character = string.charAt(index);
            }
            // 2. Else, if the first character is DQUOTE, discard it and return output.
            else if (character == '"') {
                index++;
                break;
            }
            // Else, consume the first character and append it to output.
            index++;
            stringBuilder.append(character);
        }
        // 5. Return output.
        return new QuotedParseResult(index, stringBuilder.toString());
    }

    private record QuotedParseResult(int index, String quoted) {}

    private record ParameterParseResult(int index, Map<String, String> parameters) {}

    public record LinkFieldValue(URI uri, Map<String, String> parameters) {}
}
