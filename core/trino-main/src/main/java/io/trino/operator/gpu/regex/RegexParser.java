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
package io.trino.operator.gpu.regex;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.trino.operator.gpu.regex.Pattern.Alternation;
import io.trino.operator.gpu.regex.Pattern.AnyCharacter;
import io.trino.operator.gpu.regex.Pattern.CapturingGroup;
import io.trino.operator.gpu.regex.Pattern.CharacterClass;
import io.trino.operator.gpu.regex.Pattern.CharacterClass.CodePointRange;
import io.trino.operator.gpu.regex.Pattern.InputEnd;
import io.trino.operator.gpu.regex.Pattern.InputStart;
import io.trino.operator.gpu.regex.Pattern.LineCharacter;
import io.trino.operator.gpu.regex.Pattern.LineEnd;
import io.trino.operator.gpu.regex.Pattern.LineStart;
import io.trino.operator.gpu.regex.Pattern.Literal;
import io.trino.operator.gpu.regex.Pattern.Repeat;
import io.trino.operator.gpu.regex.Pattern.Sequence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static com.google.common.collect.Iterators.singletonIterator;
import static io.trino.operator.gpu.regex.Pattern.Repeat.Greediness.GREEDY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNullElse;

public final class RegexParser
{
    private RegexParser() {}

    private static final int EOF = -1;

    public static Pattern parse(String regexp)
    {
        try {
            PeekingIterator<InputChar> input = input(regexp);
            Pattern parsed = parseAlternation(input, new Context(), new ParseState());
            consume(input, EOF);
            return parsed;
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            throw new ParsingException("Parse failed: " + requireNonNullElse(e.getMessage(), e), e);
        }
    }

    private static Pattern parseAlternation(PeekingIterator<InputChar> input, Context context, ParseState state)
    {
        ImmutableList.Builder<Pattern> alternatives = ImmutableList.builder();
        alternatives.add(parseSequence(input, context, state));
        while (input.peek().codePoint() == '|') {
            input.next();
            alternatives.add(parseSequence(input, context, state));
        }
        List<Pattern> list = alternatives.build();
        if (list.size() == 1) {
            return getOnlyElement(list);
        }
        return new Alternation(list);
    }

    private static Pattern parseSequence(PeekingIterator<InputChar> input, Context context, ParseState state)
    {
        ImmutableList.Builder<Pattern> items = ImmutableList.builder();
        while (true) {
            int next = input.peek().codePoint();
            if (next == EOF || next == ')' || next == '|') {
                break;
            }
            items.add(parseQuantified(input, context, state));
        }
        List<Pattern> list = items.build();
        if (list.size() == 1) {
            return getOnlyElement(list);
        }
        return new Sequence(list);
    }

    private static Pattern parseQuantified(PeekingIterator<InputChar> input, Context context, ParseState state)
    {
        Pattern atom = parseAtom(input, context, state);
        return switch (input.peek().codePoint()) {
            case '?' -> {
                input.next();
                yield new Repeat(atom, 0, OptionalInt.of(1), GREEDY);
            }
            case '*' -> {
                input.next();
                yield new Repeat(atom, 0, OptionalInt.empty(), GREEDY);
            }
            case '+' -> {
                input.next();
                yield new Repeat(atom, 1, OptionalInt.empty(), GREEDY);
            }
            default -> atom;
        };
    }

    private static Pattern parseAtom(PeekingIterator<InputChar> input, Context context, ParseState state)
    {
        InputChar token = input.next();
        return switch (token.codePoint()) {
            case EOF -> throw parseError(token.offset(), "Unexpected end of input");
            case '.' -> context.dotAll() ? new AnyCharacter() : new LineCharacter();
            case '^' -> context.multiline() ? new LineStart() : new InputStart();
            case '$' -> context.multiline() ? new LineEnd() : new InputEnd();
            case '\\' -> {
                InputChar escaped = input.next();
                yield switch (escaped.codePoint()) {
                    case '\\', '^', '$', '.', '|', '?', '*', '+', '(', ')', '{', '}', '[', ']' -> new Literal(escaped.codePoint());
                    default -> throw parseError(token.offset(), "Unrecognized escape");
                };
            }
            case '(' -> {
                Pattern group = parseGroupBody(input, context, state);
                consume(input, ')');
                yield group;
            }
            case '[' -> {
                Pattern charClass = parseCharClassBody(input);
                consume(input, ']');
                yield charClass;
            }
            case '|', '?', '*', '+', '{', '}', ')', ']' -> throw parseError(token.offset(), "Unexpected character: " + display(token.codePoint()));
            default -> new Literal(token.codePoint());
        };
    }

    private static Pattern parseGroupBody(PeekingIterator<InputChar> input, Context context, ParseState state)
    {
        if (input.peek().codePoint() == '?') {
            input.next();
            // non-capturing group
            consume(input, ':');
            return parseAlternation(input, context, state);
        }
        int index = state.allocateGroupIndex();
        Pattern body = parseAlternation(input, context, state);
        return new CapturingGroup(index, Optional.empty(), body);
    }

    private static Pattern parseCharClassBody(PeekingIterator<InputChar> input)
    {
        boolean negated = false;
        if (input.peek().codePoint() == '^') {
            input.next();
            negated = true;
        }
        List<CodePointRange> ranges = new ArrayList<>();
        if (input.peek().codePoint() == '-') {
            // '-' at the start of the character class
            input.next();
            ranges.add(new CodePointRange('-', '-'));
        }
        while (true) {
            switch (input.peek().codePoint()) {
                case EOF, ':', '[', '&', '(', ')', '{', '}' -> {
                    InputChar unexpected = input.next();
                    throw parseError(unexpected.offset(), "Unexpected character in character class: " + display(unexpected.codePoint()));
                }
                case '-' -> {
                    // accept '-' at the end of character class
                    InputChar hyphen = input.next();
                    if (input.peek().codePoint() != ']') {
                        throw parseError(hyphen.offset(), "Unexpected character in character class: " + display(hyphen.codePoint()));
                    }
                    ranges.add(new CodePointRange('-', '-'));
                }
                case '\\' -> {
                    InputChar escape = input.next();
                    InputChar escaped = input.next();
                    switch (escaped.codePoint()) {
                        case '\\', '^', '-', '$', '.', '|', '?', '*', '+', '(', ')', '{', '}', '[', ']' -> ranges.add(new CodePointRange(escaped.codePoint(), escaped.codePoint()));
                        default -> throw parseError(escape.offset(), "Unrecognized escape in character class: " + display(escaped.codePoint()));
                    }
                }
                default -> {
                    // `]` at the start of the character class is a literal `]`, not the class end. Anywhere else it ends the class.
                    if (input.peek().codePoint() == ']' && !ranges.isEmpty()) {
                        return new CharacterClass(negated, ranges);
                    }
                    int start = input.next().codePoint();
                    if (input.peek().codePoint() == '-') {
                        input.next();
                        switch (input.peek().codePoint()) {
                            case EOF, '\\', ':', '&', '^', '-', '$', '.', '|', '?', '*', '+', '(', ')', '{', '}', '[' -> {
                                InputChar unexpected = input.next();
                                throw parseError(unexpected.offset(), "Unexpected character in character class: " + display(unexpected.codePoint()));
                            }
                            case ']' -> {
                                // accept '-' at the end of character class
                                ranges.add(new CodePointRange(start, start));
                                ranges.add(new CodePointRange('-', '-'));
                            }
                            default -> {
                                int end = input.next().codePoint();
                                ranges.add(new CodePointRange(start, end));
                            }
                        }
                    }
                    else {
                        ranges.add(new CodePointRange(start, start));
                    }
                }
            }
        }
    }

    private static void consume(PeekingIterator<InputChar> input, int codePoint)
    {
        InputChar token = input.peek();
        if (token.codePoint() != codePoint) {
            throw parseError(token.offset(), format("Expected %s but found %s", display(codePoint), display(token.codePoint())));
        }
        input.next();
    }

    private static String display(int codePoint)
    {
        if (codePoint == EOF) {
            return "EOF";
        }
        if (codePoint >= ' ' && codePoint < 0x7F) {
            return "'%s'".formatted((char) codePoint);
        }
        return format("\\u%04X", codePoint);
    }

    private static ParsingException parseError(int offset, String message)
    {
        return new ParsingException("Parse error at offset %s: %s".formatted(offset, message));
    }

    private static final class ParseState
    {
        private int nextGroupIndex = 1;

        int allocateGroupIndex()
        {
            return nextGroupIndex++;
        }
    }

    private record Context(boolean dotAll, boolean multiline)
    {
        Context()
        {
            this(false, false);
        }
    }

    private static PeekingIterator<InputChar> input(String input)
    {
        Iterator<InputChar> chars = new AbstractIterator<>()
        {
            int nextOffset;

            @Override
            protected InputChar computeNext()
            {
                if (nextOffset == input.length()) {
                    return endOfData();
                }
                int codePoint = input.codePointAt(nextOffset);
                int offset = nextOffset;
                nextOffset = input.offsetByCodePoints(nextOffset, 1);
                return new InputChar(codePoint, offset);
            }
        };
        Iterator<InputChar> sentinel = singletonIterator(new InputChar(EOF, input.length()));
        return peekingIterator(Iterators.concat(chars, sentinel));
    }

    private record InputChar(int codePoint, int offset) {}

    public static class ParsingException
            extends RuntimeException
    {
        private ParsingException(String message)
        {
            super(message);
        }

        private ParsingException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
