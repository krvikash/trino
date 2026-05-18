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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Character.isValidCodePoint;
import static java.util.Objects.requireNonNull;

public sealed interface Pattern
{
    /**
     * Matches input if it can be decomposed into a sequence of sub-inputs that match the given patterns in order.
     */
    record Sequence(List<Pattern> items)
            implements Pattern
    {
        public Sequence
        {
            items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
        }
    }

    record Literal(int codePoint)
            implements Pattern
    {
        public Literal
        {
            checkArgument(isValidCodePoint(codePoint), "Invalid code point: %s", codePoint);
        }
    }

    /**
     * Matches any single input character, including the new line character.
     */
    record AnyCharacter()
            implements Pattern {}

    /**
     * Matches any single input character, except the new line character.
     */
    record LineCharacter()
            implements Pattern {}

    /**
     * Matches empty sequence at the start of the input.
     */
    record InputStart()
            implements Pattern {}

    /**
     * Matches empty sequence at the end of the input.
     */
    record InputEnd()
            implements Pattern {}

    /**
     * Matches empty sequence at the start of the line.
     */
    record LineStart()
            implements Pattern {}

    /**
     * Matches empty sequence at the end of the line.
     */
    record LineEnd()
            implements Pattern {}

    /**
     * Matches {@code pattern} at least {@code minOccurrences} and at most {@code maxOccurrences} times.
     */
    record Repeat(Pattern pattern, int minOccurrences, OptionalInt maxOccurrences, Greediness greediness)
            implements Pattern
    {
        public enum Greediness
        {
            /**
             * Match as many occurrences as possible, giving up occurrences on backtracking.
             */
            GREEDY,
            /**
             * Match as few occurrences as possible, taking more occurrences on backtracking.
             */
            LAZY,
            /**
             * Match as many occurrences as possible, without giving them up on backtracking.
             */
            POSSESSIVE,
        }

        public Repeat
        {
            requireNonNull(pattern, "pattern is null");
            checkArgument(0 <= minOccurrences, "Invalid min occurrences: %s", minOccurrences);
            maxOccurrences.ifPresent(max -> checkArgument(minOccurrences <= max, "Invalid min/max occurrences: %s, %s", minOccurrences, max));
            requireNonNull(greediness, "greediness is null");
        }
    }

    record Alternation(List<Pattern> alternatives)
            implements Pattern
    {
        public Alternation
        {
            alternatives = ImmutableList.copyOf(requireNonNull(alternatives, "alternatives is null"));
            checkArgument(!alternatives.isEmpty(), "No alternatives");
        }
    }

    /**
     * Matches input if {@code body} matches the input and produces a capture identified by index and optional name.
     */
    record CapturingGroup(int index, Optional<String> name, Pattern body)
            implements Pattern
    {
        public CapturingGroup
        {
            checkArgument(index > 0, "Invalid group index: %s", index);
            requireNonNull(name, "name is null");
            requireNonNull(body, "body is null");
        }
    }

    /**
     * Matches input that is same as that matched by {@code index}'th group.
     */
    record IndexedGroupReference(int index)
            implements Pattern
    {
        public IndexedGroupReference
        {
            checkArgument(index > 0, "Invalid group index: %s", index);
        }
    }

    /**
     * Matches input that is same as that matched by the group named {@code name}.
     */
    record NamedGroupReference(String name)
            implements Pattern
    {
        public NamedGroupReference
        {
            requireNonNull(name, "name is null");
        }
    }

    /**
     * Matches any single input code point that belongs to (or, when {@code negated}, does not belong to)
     * the union of the given code point ranges.
     */
    record CharacterClass(boolean negated, List<CodePointRange> ranges)
            implements Pattern
    {
        public CharacterClass
        {
            ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));
            checkArgument(!ranges.isEmpty(), "No ranges");
        }

        public record CodePointRange(int startCodePoint, int endCodePoint)
        {
            public CodePointRange
            {
                checkArgument(isValidCodePoint(startCodePoint), "Invalid start code point: %s", startCodePoint);
                checkArgument(isValidCodePoint(endCodePoint), "Invalid end code point: %s", endCodePoint);
                checkArgument(startCodePoint <= endCodePoint, "Invalid range: %s..%s", startCodePoint, endCodePoint);
            }
        }
    }
}
