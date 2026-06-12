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
package io.trino.sql.planner.exploratory;

import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.Cost;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.Property;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Candidate for a {@code (Group, Requirement)} state.
 * <p>
 * It consists of an {@link OperationImplementationAlternative} with an
 * {@link EnforcerPipeline} needed to satisfy the incoming requirement.
 */
public final class Candidate<C extends Cost, R extends Requirement, P extends Property>
{
    private final OperationImplementationAlternative<C, R, P> implementation;
    private final EnforcerPipeline<C, R, P> enforcerPipeline;
    private final C cost;

    public Candidate(
            OperationImplementationAlternative<C, R, P> implementation,
            EnforcerPipeline<C, R, P> enforcerPipeline,
            CostDomain<C> costs)
    {
        this.implementation = requireNonNull(implementation, "implementation is null");
        this.enforcerPipeline = requireNonNull(enforcerPipeline, "enforcerPipeline is null");
        requireNonNull(costs, "costs is null");
        this.cost = costs.add(implementation.cost(), enforcerPipeline.cost());
    }

    public OperationImplementationAlternative<C, R, P> implementation()
    {
        return implementation;
    }

    public EnforcerPipeline<C, R, P> enforcerPipeline()
    {
        return enforcerPipeline;
    }

    public AlternativeKey<R> alternativeKey()
    {
        return implementation.key();
    }

    public C cost()
    {
        return cost;
    }

    @Override
    public boolean equals(Object other)
    {
        return other == this || (other instanceof Candidate<?, ?, ?> candidate &&
                implementation.equals(candidate.implementation) &&
                enforcerPipeline.equals(candidate.enforcerPipeline));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(implementation, enforcerPipeline);
    }

    @Override
    public String toString()
    {
        return "Candidate[implementation=" + implementation + ", enforcerPipeline=" + enforcerPipeline + ']';
    }

    // not a record so that we can enforce canonicalization on construction
    public static final class CandidateKey<R extends Requirement>
    {
        private final AlternativeKey<R> alternativeKey;
        private final R requirement;

        public static <R extends Requirement> CandidateKey<R> candidateKey(
                AlternativeKey<R> alternativeKey,
                R requirement,
                RequirementDomain<R> requirements)
        {
            requireNonNull(requirements, "requirements is null");
            return new CandidateKey<>(alternativeKey, requirements.canonicalize(requirement));
        }

        private CandidateKey(AlternativeKey<R> alternativeKey, R requirement)
        {
            this.alternativeKey = requireNonNull(alternativeKey, "alternativeKey is null");
            // requirement is canonical; use candidateKey(...) for external callers
            this.requirement = requireNonNull(requirement, "requirement is null");
        }

        public AlternativeKey<R> alternativeKey()
        {
            return alternativeKey;
        }

        public R requirement()
        {
            return requirement;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof CandidateKey<?> otherCandidateKey)) {
                return false;
            }
            return alternativeKey.equals(otherCandidateKey.alternativeKey) &&
                    requirement.equals(otherCandidateKey.requirement);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(alternativeKey, requirement);
        }

        @Override
        public String toString()
        {
            return "CandidateKey[alternativeKey=" + alternativeKey + ", requirement=" + requirement + ']';
        }
    }
}
