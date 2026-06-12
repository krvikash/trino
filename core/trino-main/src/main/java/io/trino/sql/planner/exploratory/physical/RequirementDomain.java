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

/**
 * Represents the desired properties that should be satisfied
 * by chosen implementation.
 * <p>
 * Canonical requirements are used as parts of hash-based keys such as group
 * request keys and implementation-alternative keys. Therefore, canonical
 * requirements must provide stable semantic {@link Object#equals(Object)}
 * and {@link Object#hashCode()} implementations.
 */
public interface RequirementDomain<R extends Requirement>
{
    /**
     * The default/root requirement of this domain. Implementations must
     * return a canonical value, i.e. one for which
     * {@link #canonicalize(Requirement) canonicalize(rootRequirement())}
     * equals {@code rootRequirement()}.
     */
    R rootRequirement();

    R canonicalize(R requirement);
}
