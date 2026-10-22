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
package io.trino.sql.planner.exploratory.physical;

import java.util.List;

/**
 * Cost of a program fragment or implementation alternative.
 * <p>
 * The cost domain is expected to be monotone with respect to composition:
 * adding more work must not make the result compare as strictly better.
 * In particular, aggregation through {@link #add(Cost, Cost)} and
 * {@link #addAll(List)} should preserve the intuition that additional
 * work can only make the cost worse.
 * <p>
 * Cost aggregation is also expected to be stable under reordering:
 * combining the same set of cost components in a different order should
 * produce the same result, modulo limited numeric precision. The
 * {@link #sameTier(Cost, Cost)} relation is intended to compensate for
 * such numerical noise when costs that should be considered tied differ
 * slightly due to representation or accumulation order.
 * <p>
 * The {@link #infinite()} sentinel is used to represent an unreachable or
 * currently invalid choice. This is important for correct handling of cycles
 * during fixed-point relaxation, where some candidate paths must be treated
 * as unavailable.
 * <p>
 * A cost domain provides two orders. {@link #compare(Cost, Cost)} is a partial
 * order of knowledge: it orders two costs only when one is provably no more
 * expensive than the other, and may report them incomparable.
 * {@link #decide(Cost, Cost)} is a total decision order that extends it: it
 * orders every pair of costs, so that choosing between alternatives never
 * depends on the order in which they were found.
 */
public interface CostDomain<C extends Cost>
{
    C zero();

    C infinite();

    C parameter();

    C add(C left, C right);

    C addAll(List<C> costs);

    /**
     * Compares the left cost with the right cost under a lawful partial order.
     * The result describes the left cost relative to the right cost.
     * <p>
     * Implementations must satisfy:
     * <ul>
     *     <li>reflexivity: {@code compare(a, a) == EQUAL};</li>
     *     <li>reversal consistency: reversing the operands exchanges
     *     {@code BETTER} and {@code WORSE}, while {@code EQUAL} and
     *     {@code INCOMPARABLE} remain unchanged;</li>
     *     <li>antisymmetry: if each cost is no worse than the other,
     *     comparison returns {@code EQUAL};</li>
     *     <li>transitivity: if {@code a} is {@code BETTER} than {@code b}
     *     and {@code b} is {@code BETTER} than {@code c}, then {@code a}
     *     is {@code BETTER} than {@code c}.</li>
     * </ul>
     */
    CostComparison compare(C left, C right);

    /**
     * Compares the left cost with the right cost under the total decision order
     * used to choose between alternatives. The result describes the left cost
     * relative to the right cost and is never {@code INCOMPARABLE}.
     * <p>
     * The decision order is a plan-selection policy, not an order of knowledge:
     * where {@link #compare(Cost, Cost)} cannot prove either cost cheaper, the
     * domain still decides, deterministically, which one to prefer.
     * <p>
     * Implementations must satisfy:
     * <ul>
     *     <li>totality: the result is {@code BETTER}, {@code EQUAL}, or
     *     {@code WORSE};</li>
     *     <li>reversal consistency: reversing the operands exchanges
     *     {@code BETTER} and {@code WORSE}, while {@code EQUAL} remains
     *     unchanged;</li>
     *     <li>transitivity: if {@code a} is not {@code WORSE} than {@code b}
     *     and {@code b} is not {@code WORSE} than {@code c}, then {@code a}
     *     is not {@code WORSE} than {@code c};</li>
     *     <li>extension of the partial order: if {@code compare(a, b)} is
     *     {@code BETTER} or {@code WORSE}, {@code decide(a, b)} returns the
     *     same result, and {@code decide(a, b) == EQUAL} exactly when
     *     {@code compare(a, b) == EQUAL};</li>
     *     <li>compatibility with addition: if {@code a} is not {@code WORSE}
     *     than {@code b}, then {@code add(a, c)} is not {@code WORSE} than
     *     {@code add(b, c)};</li>
     *     <li>infinity last: every cost that is not {@code EQUAL} to
     *     {@link #infinite()} is {@code BETTER} than it;</li>
     *     <li>determinism: the result depends only on the two costs, not on
     *     the order in which they were computed or seen.</li>
     * </ul>
     */
    CostComparison decide(C left, C right);

    /**
     * Whether two costs should be considered tied.
     * <p>
     * Implementations must satisfy:
     * <ul>
     *     <li>reflexivity: {@code sameTier(a, a)} is {@code true};</li>
     *     <li>symmetry: {@code sameTier(a, b) == sameTier(b, a)};</li>
     *     <li>compatibility with equality: if {@code compare(a, b) == EQUAL},
     *     then {@code sameTier(a, b)} must be {@code true}.</li>
     * </ul>
     * The relation may be broader than equality to absorb numerical noise,
     * but must not be narrower. In particular, {@code sameTier} may be true
     * when comparison returns {@code BETTER}, {@code WORSE}, or
     * {@code INCOMPARABLE}. It need not be transitive and does not add ordering
     * relationships to the partial order.
     * <p>
     * The optimizer may suppress propagation of a cost change when the old and
     * new costs are in the same tier. Implementations must therefore return
     * {@code true} only when treating the costs as interchangeable cannot
     * materially affect dependent costing or plan selection.
     */
    boolean sameTier(C left, C right);
}
