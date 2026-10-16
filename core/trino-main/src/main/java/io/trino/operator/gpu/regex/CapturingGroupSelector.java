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
import io.trino.operator.gpu.regex.Pattern.Alternation;
import io.trino.operator.gpu.regex.Pattern.AnyCharacter;
import io.trino.operator.gpu.regex.Pattern.CapturingGroup;
import io.trino.operator.gpu.regex.Pattern.CharacterClass;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Rewrites a pattern so that only the capturing group to extract stays capturing.
 */
final class CapturingGroupSelector
{
    private CapturingGroupSelector() {}

    /**
     * Returns {@code pattern} with every capturing group other than {@code group} replaced by its body. For group 0,
     * the whole match, all of them are replaced and the result is wrapped in a capturing group. Returns
     * {@code Optional.empty()} when the pattern has no such group or contains a group reference.
     */
    static Optional<Pattern> selectCapturingGroup(Pattern pattern, int group)
    {
        checkArgument(group >= 0, "group is negative: %s", group);
        if (group > countCapturingGroups(pattern)) {
            return Optional.empty();
        }
        Optional<Pattern> selected = keepOnly(pattern, group);
        if (group == 0) {
            return selected.map(whole -> new CapturingGroup(1, Optional.empty(), whole));
        }
        return selected;
    }

    static int countCapturingGroups(Pattern pattern)
    {
        return switch (pattern) {
            case Sequence(List<Pattern> items) -> items.stream().mapToInt(CapturingGroupSelector::countCapturingGroups).sum();
            case Alternation(List<Pattern> alternatives) -> alternatives.stream().mapToInt(CapturingGroupSelector::countCapturingGroups).sum();
            case Repeat repeat -> countCapturingGroups(repeat.pattern());
            case CapturingGroup capturingGroup -> 1 + countCapturingGroups(capturingGroup.body());
            case Literal _, AnyCharacter _, LineCharacter _, InputStart _, InputEnd _, LineStart _, LineEnd _, CharacterClass _, IndexedGroupReference _, NamedGroupReference _ -> 0;
        };
    }

    private static Optional<Pattern> keepOnly(Pattern pattern, int group)
    {
        return switch (pattern) {
            case Sequence(List<Pattern> items) -> keepOnly(items, group).map(Sequence::new);
            case Alternation(List<Pattern> alternatives) -> keepOnly(alternatives, group).map(Alternation::new);
            case Repeat repeat -> keepOnly(repeat.pattern(), group)
                    .map(selected -> new Repeat(selected, repeat.minOccurrences(), repeat.maxOccurrences(), repeat.greediness()));
            case CapturingGroup capturingGroup when capturingGroup.index() == group -> keepOnly(capturingGroup.body(), group)
                    .map(body -> new CapturingGroup(capturingGroup.index(), capturingGroup.name(), body));
            case CapturingGroup capturingGroup -> keepOnly(capturingGroup.body(), group);
            // The other groups are dropped, so a reference could end up pointing to a different group or to none
            case IndexedGroupReference _, NamedGroupReference _ -> Optional.empty();
            case Literal _, AnyCharacter _, LineCharacter _, InputStart _, InputEnd _, LineStart _, LineEnd _, CharacterClass _ -> Optional.of(pattern);
        };
    }

    private static Optional<List<Pattern>> keepOnly(List<Pattern> patterns, int group)
    {
        ImmutableList.Builder<Pattern> selected = ImmutableList.builderWithExpectedSize(patterns.size());
        for (Pattern pattern : patterns) {
            Optional<Pattern> item = keepOnly(pattern, group);
            if (item.isEmpty()) {
                return Optional.empty();
            }
            selected.add(item.get());
        }
        return Optional.of(selected.build());
    }
}
