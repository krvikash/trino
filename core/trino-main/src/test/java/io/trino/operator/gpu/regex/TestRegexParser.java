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

import io.trino.operator.gpu.regex.Pattern.Alternation;
import io.trino.operator.gpu.regex.Pattern.CapturingGroup;
import io.trino.operator.gpu.regex.Pattern.CharacterClass;
import io.trino.operator.gpu.regex.Pattern.CharacterClass.CodePointRange;
import io.trino.operator.gpu.regex.Pattern.InputEnd;
import io.trino.operator.gpu.regex.Pattern.InputStart;
import io.trino.operator.gpu.regex.Pattern.LineCharacter;
import io.trino.operator.gpu.regex.Pattern.Literal;
import io.trino.operator.gpu.regex.Pattern.Repeat;
import io.trino.operator.gpu.regex.Pattern.Sequence;
import io.trino.operator.gpu.regex.RegexParser.ParsingException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.operator.gpu.regex.Pattern.Repeat.Greediness.GREEDY;
import static io.trino.operator.gpu.regex.RegexParser.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRegexParser
{
    private final Sequence emptySequence = new Sequence(List.of());

    @Test
    void testLiterals()
    {
        assertThat(parse("a")).isEqualTo(new Literal('a'));
        assertThat(parse("abc")).isEqualTo(new Sequence(List.of(new Literal('a'), new Literal('b'), new Literal('c'))));
        assertThat(parse("é")).isEqualTo(new Literal('é'));
        assertThat(parse("😀")).isEqualTo(new Literal(0x1F600));
        assertThat(parse("")).isEqualTo(new Sequence(List.of()));
    }

    @Test
    void testAnchorsAndDot()
    {
        assertThat(parse("^")).isEqualTo(new InputStart());
        assertThat(parse("$")).isEqualTo(new InputEnd());
        assertThat(parse(".")).isEqualTo(new LineCharacter());
        assertThat(parse("^a.$")).isEqualTo(new Sequence(List.of(
                new InputStart(),
                new Literal('a'),
                new LineCharacter(),
                new InputEnd())));
    }

    @Test
    void testEscapes()
    {
        assertThat(parse("\\.")).isEqualTo(new Literal('.'));
        assertThat(parse("\\\\")).isEqualTo(new Literal('\\'));
        assertThat(parse("\\|")).isEqualTo(new Literal('|'));
        assertThat(parse("\\(")).isEqualTo(new Literal('('));
        assertThat(parse("\\[")).isEqualTo(new Literal('['));

        assertParseError(() -> parse("\\d")).hasMessage("Parse error at offset 0: Unrecognized escape");
        assertParseError(() -> parse("\\n")).hasMessage("Parse error at offset 0: Unrecognized escape");
        assertParseError(() -> parse("\\")).hasMessage("Parse error at offset 0: Unrecognized escape");
    }

    @Test
    void testQuantifiers()
    {
        assertThat(parse("a?")).isEqualTo(new Repeat(new Literal('a'), 0, OptionalInt.of(1), GREEDY));
        assertThat(parse("a*")).isEqualTo(new Repeat(new Literal('a'), 0, OptionalInt.empty(), GREEDY));
        assertThat(parse("a+")).isEqualTo(new Repeat(new Literal('a'), 1, OptionalInt.empty(), GREEDY));

        assertParseError(() -> parse("*")).hasMessage("Parse error at offset 0: Unexpected character: '*'");
        assertParseError(() -> parse("?")).hasMessage("Parse error at offset 0: Unexpected character: '?'");
        assertParseError(() -> parse("+")).hasMessage("Parse error at offset 0: Unexpected character: '+'");
    }

    @Test
    void testAlternation()
    {
        assertThat(parse("a|b")).isEqualTo(new Alternation(List.of(new Literal('a'), new Literal('b'))));
        assertThat(parse("a|b|c")).isEqualTo(new Alternation(List.of(new Literal('a'), new Literal('b'), new Literal('c'))));
        assertThat(parse("ab|cd")).isEqualTo(new Alternation(List.of(
                new Sequence(List.of(new Literal('a'), new Literal('b'))),
                new Sequence(List.of(new Literal('c'), new Literal('d'))))));
        assertThat(parse("|")).isEqualTo(new Alternation(List.of(emptySequence, emptySequence)));
        assertThat(parse("a|")).isEqualTo(new Alternation(List.of(new Literal('a'), emptySequence)));
        assertThat(parse("|a")).isEqualTo(new Alternation(List.of(emptySequence, new Literal('a'))));
    }

    @Test
    void testGroups()
    {
        assertThat(parse("()")).isEqualTo(new CapturingGroup(1, Optional.empty(), emptySequence));
        assertThat(parse("(a)")).isEqualTo(new CapturingGroup(1, Optional.empty(), new Literal('a')));
        assertThat(parse("(ab)")).isEqualTo(new CapturingGroup(1, Optional.empty(),
                new Sequence(List.of(new Literal('a'), new Literal('b')))));
        assertThat(parse("(a)(b)")).isEqualTo(new Sequence(List.of(
                new CapturingGroup(1, Optional.empty(), new Literal('a')),
                new CapturingGroup(2, Optional.empty(), new Literal('b')))));
        assertThat(parse("(a(b)c)")).isEqualTo(new CapturingGroup(1, Optional.empty(), new Sequence(List.of(
                new Literal('a'),
                new CapturingGroup(2, Optional.empty(), new Literal('b')),
                new Literal('c')))));

        assertThat(parse("(?:)")).isEqualTo(emptySequence);
        assertThat(parse("(?:a)")).isEqualTo(new Literal('a'));
        assertThat(parse("(?:a)(b)")).isEqualTo(new Sequence(List.of(
                new Literal('a'),
                new CapturingGroup(1, Optional.empty(), new Literal('b')))));

        assertParseError(() -> parse("(")).hasMessage("Parse error at offset 1: Expected ')' but found EOF");
        assertParseError(() -> parse(")")).hasMessage("Parse error at offset 0: Expected EOF but found ')'");
        assertParseError(() -> parse("(?a)")).hasMessage("Parse error at offset 2: Expected ':' but found 'a'");
    }

    @Test
    void testCharacterClass()
    {
        assertThat(parse("[a]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange('a', 'a'))));
        assertThat(parse("[abc]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('a', 'a'),
                new CodePointRange('b', 'b'),
                new CodePointRange('c', 'c'))));
        assertThat(parse("[a-z]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange('a', 'z'))));
        assertThat(parse("[a-z0-9]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('a', 'z'),
                new CodePointRange('0', '9'))));
        assertThat(parse("[^abc]")).isEqualTo(new CharacterClass(true, List.of(
                new CodePointRange('a', 'a'),
                new CodePointRange('b', 'b'),
                new CodePointRange('c', 'c'))));
        assertThat(parse("[-a]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('-', '-'),
                new CodePointRange('a', 'a'))));
        assertThat(parse("[a-]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('a', 'a'),
                new CodePointRange('-', '-'))));
        assertThat(parse("[a^b]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('a', 'a'),
                new CodePointRange('^', '^'),
                new CodePointRange('b', 'b'))));
        assertThat(parse("[\\.\\-]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange('.', '.'),
                new CodePointRange('-', '-'))));

        assertParseError(() -> parse("[")).hasMessage("Parse error at offset 1: Unexpected character in character class: EOF");
        assertParseError(() -> parse("[]")).hasMessage("Parse error at offset 2: Unexpected character in character class: EOF");
        assertParseError(() -> parse("[^]")).hasMessage("Parse error at offset 3: Unexpected character in character class: EOF");
        assertParseError(() -> parse("[abc")).hasMessage("Parse error at offset 4: Unexpected character in character class: EOF");
        assertParseError(() -> parse("[{]")).hasMessage("Parse error at offset 1: Unexpected character in character class: '{'");
        assertParseError(() -> parse("[[]")).hasMessage("Parse error at offset 1: Unexpected character in character class: '['");
        assertParseError(() -> parse("[\\d]")).hasMessage("Parse error at offset 1: Unrecognized escape in character class: 'd'");
        assertParseError(() -> parse("[a-b-e-f]")).hasMessage("Parse error at offset 4: Unexpected character in character class: '-'");

        // currently range ends cannot be escape or special characters
        assertParseError(() -> parse("[a-\\.]")).hasMessage("Parse error at offset 3: Unexpected character in character class: '\\'");
        assertParseError(() -> parse("[a-^]")).hasMessage("Parse error at offset 3: Unexpected character in character class: '^'");
        assertParseError(() -> parse("[a-$]")).hasMessage("Parse error at offset 3: Unexpected character in character class: '$'");

        // negative range
        assertParseError(() -> parse("[z-a]")).hasMessage("Parse failed: Invalid range: 122..97");

        assertThat(parse("[.-z]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange('.', 'z'))));
        assertThat(parse("[?-z]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange('?', 'z'))));
        assertThat(parse("[*-z]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange('*', 'z'))));
        assertParseError(() -> parse("[\\.-z]")).hasMessage("Parse error at offset 3: Unexpected character in character class: '-'");

        // ']' at the start of the character class is a literal, matching java.util.regex
        assertThat(parse("[]]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange(']', ']'))));
        assertThat(parse("[]a]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange(']', ']'),
                new CodePointRange('a', 'a'))));
        assertThat(parse("[]-a]")).isEqualTo(new CharacterClass(false, List.of(new CodePointRange(']', 'a'))));
        assertThat(parse("[]-]")).isEqualTo(new CharacterClass(false, List.of(
                new CodePointRange(']', ']'),
                new CodePointRange('-', '-'))));
        assertThat(parse("[^]]")).isEqualTo(new CharacterClass(true, List.of(new CodePointRange(']', ']'))));
        assertThat(parse("[^]a]")).isEqualTo(new CharacterClass(true, List.of(
                new CodePointRange(']', ']'),
                new CodePointRange('a', 'a'))));
    }

    @Test
    void testComplex()
    {
        assertThat(parse("a(b|c)*d")).isEqualTo(new Sequence(List.of(
                new Literal('a'),
                new Repeat(
                        new CapturingGroup(1, Optional.empty(), new Alternation(List.of(new Literal('b'), new Literal('c')))),
                        0,
                        OptionalInt.empty(),
                        GREEDY),
                new Literal('d'))));
        assertThat(parse("^[a-z]+@[a-z]+\\.[a-z]+$")).isEqualTo(new Sequence(List.of(
                new InputStart(),
                new Repeat(new CharacterClass(false, List.of(new CodePointRange('a', 'z'))), 1, OptionalInt.empty(), GREEDY),
                new Literal('@'),
                new Repeat(new CharacterClass(false, List.of(new CodePointRange('a', 'z'))), 1, OptionalInt.empty(), GREEDY),
                new Literal('.'),
                new Repeat(new CharacterClass(false, List.of(new CodePointRange('a', 'z'))), 1, OptionalInt.empty(), GREEDY),
                new InputEnd())));
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertParseError(ThrowingCallable callable)
    {
        return assertThatThrownBy(callable).isInstanceOf(ParsingException.class);
    }
}
