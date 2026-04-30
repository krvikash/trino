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
import io.trino.operator.gpu.regex.RegexParser.RegexContext;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.NodeLocation;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;
import java.util.Optional;

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

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
    };

    public static Optional<TranspileResult> transpile(String pattern, String replacement)
    {
        RegexPattern parsed;
        try {
            parsed = parse(pattern);
        }
        catch (ParsingException e) {
            log.debug("Regex pattern not supported on GPU, falling back to CPU: %s", e.getMessage());
            return Optional.empty();
        }

        PatternTranspiler patternTranspiler = new PatternTranspiler();
        return patternTranspiler.transpile(parsed).flatMap(transpiledPattern ->
                transpileReplacement(replacement, transpiledPattern.groupCount).map(result ->
                        new TranspileResult(transpiledPattern.pattern, result.replacement(), result.hasBackreferences())));
    }

    private static RegexPattern parse(String pattern)
    {
        try {
            RegexLexer lexer = new RegexLexer(CharStreams.fromString(pattern));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            RegexParser parser = new RegexParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();

            RegexContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                parser.setErrorHandler(new BailErrorStrategy());
                tree = parser.regex();
            }
            catch (ParseCancellationException _) {
                // if we fail, parse with LL mode
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                parser.setErrorHandler(new DefaultErrorStrategy());
                parser.addErrorListener(ERROR_LISTENER);
                tree = parser.regex();
            }

            return new RegexTreeBuilder().visitRegex(tree);
        }
        catch (StackOverflowError _) {
            throw new ParsingException("stack overflow while parsing regex", new NodeLocation(1, 1));
        }
    }

    private static class PatternTranspiler
    {
        private int groupCount;

        public Optional<TranspiledPattern> transpile(RegexPattern pattern)
        {
            return transpileSequence(pattern.items()).map(transpiledPattern ->
                    new TranspiledPattern(transpiledPattern, groupCount));
        }

        private Optional<String> transpileSequence(List<Quantified> items)
        {
            StringBuilder builder = new StringBuilder();
            for (Quantified item : items) {
                Optional<String> result = transpileAtom(item.atom());
                if (result.isEmpty()) {
                    return Optional.empty();
                }
                builder.append(result.get());
                item.quantifier().ifPresent(q -> builder.append(q.symbol()));
            }
            return Optional.of(builder.toString());
        }

        private Optional<String> transpileAtom(Atom atom)
        {
            return switch (atom) {
                case Literal literal -> isUnsupportedCodePoint(literal.codePoint())
                        ? Optional.empty()
                        : Optional.of(Character.toString(literal.codePoint()));
                case Dot _ -> Optional.of(".");
                case Anchor anchor -> Optional.of(Character.toString(anchor.value()));
                case Escape escape -> Optional.of("\\" + escape.escapedChar());
                case CapturingGroup group -> {
                    Optional<String> inner = transpileSequence(group.items());
                    if (inner.isEmpty()) {
                        yield Optional.empty();
                    }
                    groupCount++;
                    yield Optional.of("(" + inner.get() + ")");
                }
                case NonCapturingGroup group -> transpileSequence(group.items()).map(inner -> "(?:" + inner + ")");
                case CharClass charClass -> transpileCharClass(charClass);
            };
        }

        private Optional<String> transpileCharClass(CharClass charClass)
        {
            StringBuilder content = new StringBuilder();
            for (CharClassAtom item : charClass.items()) {
                Optional<String> transpiled = transpileCharClassAtom(item);
                if (transpiled.isEmpty()) {
                    return Optional.empty();
                }
                content.append(transpiled.get());
            }

            if (charClass.negated()) {
                return Optional.of("[^" + content + "]");
            }
            return Optional.of("[" + content + "]");
        }

        private static Optional<String> transpileCharClassAtom(CharClassAtom atom)
        {
            return switch (atom) {
                // In cuDF, unescaped hyphens in character classes may lead to undefined behavior.
                case CharLiteral literal -> isUnsupportedCodePoint(literal.codePoint()) || literal.codePoint() == '-'
                        ? Optional.empty()
                        : Optional.of(Character.toString(literal.codePoint()));
                case CharEscape escape -> Optional.of("\\" + escape.escapedChar());
                case CharRange range -> isUnsupportedCodePoint(range.startCodePoint()) || isUnsupportedCodePoint(range.endCodePoint())
                        ? Optional.empty()
                        : Optional.of(Character.toString(range.startCodePoint()) + "-" + Character.toString(range.endCodePoint()));
            };
        }

        private static boolean isUnsupportedCodePoint(int codePoint)
        {
            // cuDF regex matching is limited to BMP (U+0000 to U+FFFF)
            // https://docs.rapids.ai/api/cudf/stable/libcudf_docs/unicode_limitations/
            return codePoint > 0xFFFF;
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
