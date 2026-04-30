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

import com.google.common.collect.ImmutableList;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.NodeLocation;

import java.util.List;
import java.util.Optional;

import static io.trino.operator.gpu.regex.Quantifier.ONE_OR_MORE;
import static io.trino.operator.gpu.regex.Quantifier.ZERO_OR_MORE;
import static io.trino.operator.gpu.regex.Quantifier.ZERO_OR_ONE;

class RegexTreeBuilder
        extends RegexBaseVisitor<Object>
{
    @Override
    public RegexPattern visitRegex(RegexParser.RegexContext ctx)
    {
        return new RegexPattern(visitSequence(ctx.sequence()));
    }

    @Override
    public List<Quantified> visitSequence(RegexParser.SequenceContext ctx)
    {
        ImmutableList.Builder<Quantified> items = ImmutableList.builder();
        for (RegexParser.QuantifiedContext q : ctx.quantified()) {
            items.add(visitQuantified(q));
        }
        return items.build();
    }

    @Override
    public Quantified visitQuantified(RegexParser.QuantifiedContext ctx)
    {
        Atom atom = (Atom) visit(ctx.atom());
        Optional<Quantifier> quantifier = Optional.empty();
        if (ctx.quantifier() != null) {
            quantifier = Optional.of(visitQuantifier(ctx.quantifier()));
        }
        return new Quantified(atom, quantifier);
    }

    @Override
    public Quantifier visitQuantifier(RegexParser.QuantifierContext ctx)
    {
        return switch (ctx.getText()) {
            case "?" -> ZERO_OR_ONE;
            case "*" -> ZERO_OR_MORE;
            case "+" -> ONE_OR_MORE;
            default -> throw new IllegalArgumentException("Unknown quantifier: " + ctx.getText());
        };
    }

    @Override
    public Atom visitAtom(RegexParser.AtomContext ctx)
    {
        if (ctx.DOT() != null) {
            return new Dot();
        }
        return (Atom) visitChildren(ctx);
    }

    @Override
    public Literal visitLiteral(RegexParser.LiteralContext ctx)
    {
        return new Literal(toCodePoint(ctx.getText()));
    }

    @Override
    public Anchor visitAnchor(RegexParser.AnchorContext ctx)
    {
        return new Anchor(toChar(ctx.getText()));
    }

    @Override
    public Escape visitEscape(RegexParser.EscapeContext ctx)
    {
        return new Escape(toEscapedChar(ctx.getText()));
    }

    @Override
    public CapturingGroup visitCapturingGroup(RegexParser.CapturingGroupContext ctx)
    {
        return new CapturingGroup(visitSequence(ctx.sequence()));
    }

    @Override
    public NonCapturingGroup visitNonCapturingGroup(RegexParser.NonCapturingGroupContext ctx)
    {
        return new NonCapturingGroup(visitSequence(ctx.sequence()));
    }

    @Override
    public CharClass visitNegatedCharClass(RegexParser.NegatedCharClassContext ctx)
    {
        return new CharClass(true, visitCharClassBody(ctx.charClassBody()));
    }

    @Override
    public CharClass visitPositiveCharClass(RegexParser.PositiveCharClassContext ctx)
    {
        return new CharClass(false, visitCharClassBody(ctx.charClassBody()));
    }

    @Override
    public List<CharClassAtom> visitCharClassBody(RegexParser.CharClassBodyContext ctx)
    {
        ImmutableList.Builder<CharClassAtom> items = ImmutableList.builder();
        for (RegexParser.CharClassAtomContext atom : ctx.charClassAtom()) {
            items.add((CharClassAtom) visit(atom));
        }
        return items.build();
    }

    @Override
    public CharClassAtom visitCharClassLiteralOrRange(RegexParser.CharClassLiteralOrRangeContext ctx)
    {
        if (ctx.HYPHEN() != null) {
            return new CharRange(toCodePoint(ctx.charClassLiteral(0).getText()), toCodePoint(ctx.charClassLiteral(1).getText()));
        }
        return new CharLiteral(toCodePoint(ctx.charClassLiteral(0).getText()));
    }

    @Override
    public CharEscape visitCharClassEscapeAtom(RegexParser.CharClassEscapeAtomContext ctx)
    {
        return visitCharClassEscape(ctx.charClassEscape());
    }

    @Override
    public CharEscape visitCharClassEscape(RegexParser.CharClassEscapeContext ctx)
    {
        return new CharEscape(toEscapedChar(ctx.getText()));
    }

    private static int toCodePoint(String text)
    {
        if (text.codePointCount(0, text.length()) != 1) {
            throw parseError("Expected single code point: " + text);
        }
        return text.codePointAt(0);
    }

    private static char toChar(String text)
    {
        if (text.length() != 1) {
            throw parseError("Expected single character: " + text);
        }
        return text.charAt(0);
    }

    private static char toEscapedChar(String text)
    {
        if (text.length() != 2 || text.charAt(0) != '\\') {
            throw parseError("Expected escaped character: " + text);
        }
        return text.charAt(1);
    }

    private static ParsingException parseError(String message)
    {
        return new ParsingException(message, new NodeLocation(1, 1));
    }
}
