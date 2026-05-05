/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package io.starburst.schema.discovery.processor;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

/**
 * A class for POSIX glob pattern with brace expansions.
 * <p>
 * Note: copied from {@code org.apache.hadoop.fs.GlobPattern} as {@code org.apache.hadoop} dependency
 * was removed and this class isn't available anymore. That's only leftover from hadoop which is
 * needed in schema discovery, and there is no alternative in Trino
 */
public class GlobPattern
{
    private static final char BACKSLASH = '\\';

    private final Pattern compiled;

    /**
     * Construct the glob pattern object with a glob pattern string
     *
     * @param globPattern the glob pattern string
     */
    public GlobPattern(String globPattern)
    {
        compiled = compile(globPattern);
    }

    /**
     * Match input against the compiled glob pattern
     *
     * @return true for successful matches
     */
    public boolean matches(String s)
    {
        return compiled.matcher(s).matches();
    }

    private static Pattern compile(String glob)
    {
        StringBuilder regex = new StringBuilder();
        int setOpen = 0;
        int curlyOpen = 0;
        int len = glob.length();

        for (int i = 0; i < len; i++) {
            char c = glob.charAt(i);

            boolean appendCurrent = switch (c) {
                case BACKSLASH -> {
                    if (++i >= len) {
                        error("Missing escaped character", glob, i);
                    }
                    regex.append(c).append(glob.charAt(i));
                    yield false;
                }
                // escape regex special chars that are not glob special chars
                case '.', '$', '(', ')', '|', '+' -> {
                    regex.append(BACKSLASH);
                    yield true;
                }
                case '*' -> {
                    regex.append('.');
                    yield true;
                }
                case '?' -> {
                    regex.append('.');
                    yield false;
                }
                // start of a group
                case '{' -> {
                    regex.append("(?:"); // non-capturing
                    curlyOpen++;
                    yield false;
                }
                case ',' -> {
                    regex.append(curlyOpen > 0 ? '|' : c);
                    yield false;
                }
                case '}' -> {
                    if (curlyOpen > 0) {
                        // end of a group
                        curlyOpen--;
                        regex.append(")");
                        yield false;
                    }
                    yield true;
                }
                case '[' -> {
                    if (setOpen > 0) {
                        error("Unclosed character class", glob, i);
                    }
                    setOpen++;
                    yield true;
                }
                // ^ inside [...] can be unescaped
                case '^' -> {
                    if (setOpen == 0) {
                        regex.append(BACKSLASH);
                    }
                    yield true;
                }
                // [! needs to be translated to [^
                case '!' -> {
                    regex.append(setOpen > 0 && '[' == glob.charAt(i - 1) ? '^' : '!');
                    yield false;
                }
                // Many set errors like [][] could not be easily detected here,
                // as []], []-] and [-] are all valid POSIX glob and java regex.
                // We'll just let the regex compiler do the real work.
                case ']' -> {
                    setOpen = 0;
                    yield true;
                }
                default -> true;
            };
            if (appendCurrent) {
                regex.append(c);
            }
        }

        if (setOpen > 0) {
            error("Unclosed character class", glob, len);
        }
        if (curlyOpen > 0) {
            error("Unclosed group", glob, len);
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }

    private static void error(String message, String pattern, int pos)
    {
        String fullMessage = String.format("%s at pos %d", message, pos);
        throw new PatternSyntaxException(fullMessage, pattern);
    }
}
