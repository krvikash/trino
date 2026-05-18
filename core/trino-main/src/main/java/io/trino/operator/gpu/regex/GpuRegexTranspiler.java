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

import io.airlift.log.Logger;
import io.trino.operator.gpu.regex.Pattern.Alternation;
import io.trino.operator.gpu.regex.Pattern.AnyCharacter;
import io.trino.operator.gpu.regex.Pattern.CapturingGroup;
import io.trino.operator.gpu.regex.Pattern.CharacterClass;
import io.trino.operator.gpu.regex.Pattern.CharacterClass.CodePointRange;
import io.trino.operator.gpu.regex.Pattern.IndexedGroupReference;
import io.trino.operator.gpu.regex.Pattern.InputEnd;
import io.trino.operator.gpu.regex.Pattern.InputStart;
import io.trino.operator.gpu.regex.Pattern.LineCharacter;
import io.trino.operator.gpu.regex.Pattern.LineEnd;
import io.trino.operator.gpu.regex.Pattern.LineStart;
import io.trino.operator.gpu.regex.Pattern.Literal;
import io.trino.operator.gpu.regex.Pattern.NamedGroupReference;
import io.trino.operator.gpu.regex.Pattern.Repeat;
import io.trino.operator.gpu.regex.Pattern.Sequence;
import io.trino.operator.gpu.regex.RegexParser.ParsingException;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.operator.gpu.regex.Pattern.Repeat.Greediness.GREEDY;
import static java.util.Objects.requireNonNull;

/**
 * Transpiles a Trino regex pattern and replacement string to cuDF-compatible syntax.
 * <p>
 * Returns {@code Optional.empty()} for pattern/replacement combinations that cannot be safely
 * executed on GPU.
 * <p>
 * For more details about cuDF regex syntax see <a href="https://docs.rapids.ai/api/cudf/stable/libcudf_docs/md_regex/#regex-features">the official documentation</a>.
 */
public final class GpuRegexTranspiler
{
    private static final Logger log = Logger.get(GpuRegexTranspiler.class);

    private GpuRegexTranspiler() {}

    public static Optional<TranspileResult> transpile(String pattern, String replacement)
    {
        Pattern parsed;
        try {
            parsed = RegexParser.parse(pattern);
        }
        catch (ParsingException e) {
            log.debug("Regex pattern not supported on GPU, falling back to CPU: %s", e.getMessage());
            return Optional.empty();
        }

        PatternTranspiler transpiler = new PatternTranspiler();
        return transpiler.transpile(parsed).flatMap(transpiled ->
                transpileReplacement(replacement, transpiled.groupCount()).map(result ->
                        new TranspileResult(transpiled.pattern(), result.replacement(), result.hasBackreferences())));
    }

    private static final class PatternTranspiler
    {
        private int groupCount;

        Optional<TranspiledPattern> transpile(Pattern pattern)
        {
            return render(pattern).map(rendered -> new TranspiledPattern(rendered, groupCount));
        }

        private Optional<String> render(Pattern pattern)
        {
            return switch (pattern) {
                case Sequence(List<Pattern> items) -> {
                    if (items.isEmpty()) {
                        // TODO empty sequences currently blocked from conversion because cudf behaves differently, at least for regexp_replace
                        yield Optional.empty();
                    }
                    yield renderItems(items).map(s -> "(?:" + s + ")");
                }
                case Alternation(List<Pattern> alternatives) -> renderAlternation(alternatives).map(s -> "(?:" + s + ")");
                case Repeat repeat -> renderRepeat(repeat);
                case CapturingGroup group -> renderCapturingGroup(group.body());
                case CharacterClass charClass -> renderCharacterClass(charClass);
                case Literal(int codePoint) -> renderLiteral(codePoint);
                case LineCharacter _ -> Optional.of(".");
                case InputStart _ -> Optional.of("^");
                case InputEnd _ -> Optional.of("$");
                case AnyCharacter _, LineStart _, LineEnd _, IndexedGroupReference _, NamedGroupReference _ -> Optional.empty();
            };
        }

        private Optional<String> renderItems(List<Pattern> items)
        {
            StringBuilder builder = new StringBuilder();
            for (Pattern item : items) {
                Optional<String> rendered = render(item);
                if (rendered.isEmpty()) {
                    return Optional.empty();
                }
                builder.append(rendered.get());
            }
            return Optional.of(builder.toString());
        }

        private Optional<String> renderAlternation(List<Pattern> alternatives)
        {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < alternatives.size(); i++) {
                if (i > 0) {
                    builder.append('|');
                }
                Optional<String> rendered = render(alternatives.get(i));
                if (rendered.isEmpty()) {
                    return Optional.empty();
                }
                builder.append(rendered.get());
            }
            return Optional.of(builder.toString());
        }

        private Optional<String> renderRepeat(Repeat repeat)
        {
            if (repeat.greediness() != GREEDY) {
                // TODO support other greediness modes
                return Optional.empty();
            }
            return renderQuantifier(repeat.minOccurrences(), repeat.maxOccurrences()).flatMap(quantifier ->
                    render(repeat.pattern()).map(inner -> inner + quantifier));
        }

        private static Optional<String> renderQuantifier(int min, OptionalInt max)
        {
            if (min == 0 && max.equals(OptionalInt.of(1))) {
                return Optional.of("?");
            }
            if (min == 0 && max.isEmpty()) {
                return Optional.of("*");
            }
            if (min == 1 && max.isEmpty()) {
                return Optional.of("+");
            }
            return Optional.empty();
        }

        private Optional<String> renderCapturingGroup(Pattern body)
        {
            Optional<String> rendered = render(body);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            groupCount++;
            return Optional.of("(" + rendered.get() + ")");
        }

        private static Optional<String> renderLiteral(int codePoint)
        {
            return switch (codePoint) {
                case '\\', '^', '$', '.', '|', '?', '*', '+', '(', ')', '{', '}', '[', ']' -> Optional.of("\\" + Character.toString(codePoint));
                default -> {
                    if (isUnsupportedCodePoint(codePoint)) {
                        yield Optional.empty();
                    }
                    yield Optional.of(Character.toString(codePoint));
                }
            };
        }

        private static Optional<String> renderCharacterClass(CharacterClass charClass)
        {
            StringBuilder content = new StringBuilder(charClass.negated() ? "[^" : "[");
            for (CodePointRange range : charClass.ranges()) {
                if (isUnsupportedCodePoint(range.startCodePoint()) || isUnsupportedCodePoint(range.endCodePoint())) {
                    return Optional.empty();
                }
                if (range.startCodePoint() == range.endCodePoint()) {
                    content.append(escapeInsideCharClass(range.startCodePoint()));
                }
                else {
                    content.append(escapeInsideCharClass(range.startCodePoint()))
                            .append('-')
                            .append(escapeInsideCharClass(range.endCodePoint()));
                }
            }
            content.append("]");
            return Optional.of(content.toString());
        }

        private static String escapeInsideCharClass(int codePoint)
        {
            return switch (codePoint) {
                case '\\', '^', '-', ']' -> "\\" + Character.toString(codePoint);
                default -> Character.toString(codePoint);
            };
        }

        private static boolean isUnsupportedCodePoint(int codePoint)
        {
            // cuDF regex matching is limited to BMP (U+0000 to U+FFFF)
            // https://docs.rapids.ai/api/cudf/stable/libcudf_docs/unicode_limitations/
            return codePoint == 0 || codePoint > 0xFFFF;
        }
    }

    private static Optional<ReplacementResult> transpileReplacement(String replacement, int patternGroupCount)
    {
        StringBuilder output = new StringBuilder(replacement.length());
        boolean hasBackreferences = false;
        boolean hasLiteralBackslash = false;

        int i = 0;
        while (i < replacement.length()) {
            char c = replacement.charAt(i);

            if (c == '$') {
                i++;
                if (i >= replacement.length()) {
                    return Optional.empty();
                }

                char next = replacement.charAt(i);
                if (next == '{') {
                    return Optional.empty();
                }

                if (next < '0' || next > '9') {
                    return Optional.empty();
                }

                int groupNumber = next - '0';
                if (groupNumber > patternGroupCount) {
                    return Optional.empty();
                }
                i++;

                while (i < replacement.length() && groupNumber <= 99) {
                    char digit = replacement.charAt(i);
                    if (digit < '0' || digit > '9') {
                        break;
                    }
                    int newGroupNum = groupNumber * 10 + (digit - '0');
                    if (newGroupNum > patternGroupCount) {
                        break;
                    }
                    groupNumber = newGroupNum;
                    i++;
                }

                if (groupNumber > 99) {
                    return Optional.empty();
                }

                output.append("${");
                output.append(groupNumber);
                output.append('}');
                hasBackreferences = true;
            }
            else if (c == '\\') {
                i++;
                if (i >= replacement.length()) {
                    return Optional.empty();
                }
                char escaped = replacement.charAt(i);
                if (escaped == '$') {
                    output.append('$');
                }
                else if (escaped == '\\') {
                    output.append('\\');
                    hasLiteralBackslash = true;
                }
                else {
                    // \$ and \\ are the only meaningful escapes in Java's
                    // Matcher.appendReplacement. Reject anything else rather
                    // than guessing cuDF semantics.
                    return Optional.empty();
                }
                i++;
            }
            else {
                output.append(c);
                i++;
            }
        }

        // cuDF's stringReplaceWithBackrefs treats \ followed by a digit as a
        // backreference and has no escape mechanism for literal backslashes.
        // We cannot safely emit a literal backslash in a template that also
        // contains backreferences — fall back to CPU.
        if (hasBackreferences && hasLiteralBackslash) {
            return Optional.empty();
        }
        return Optional.of(new ReplacementResult(output.toString(), hasBackreferences));
    }

    public record TranspileResult(String pattern, String replacement, boolean hasBackreferences)
    {
        public TranspileResult
        {
            requireNonNull(pattern, "pattern is null");
            requireNonNull(replacement, "replacement is null");
        }
    }

    private record ReplacementResult(String replacement, boolean hasBackreferences)
    {
        public ReplacementResult
        {
            requireNonNull(replacement, "replacement is null");
        }
    }

    private record TranspiledPattern(String pattern, int groupCount)
    {
        public TranspiledPattern
        {
            requireNonNull(pattern, "pattern is null");
        }
    }
}
