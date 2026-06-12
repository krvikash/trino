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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSubstitution
{
    @Test
    void testIdentityForUnknownKey()
    {
        var substitution = new Substitution<Integer>();
        assertThat(substitution.getOrIdentity(42)).isEqualTo(42);

        substitution.put(1, 2);
        assertThat(substitution.getOrIdentity(42)).isEqualTo(42);
    }

    @Test
    void testSingleMapping()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(2);
        assertThat(substitution.getOrIdentity(2)).isEqualTo(2);
    }

    @Test
    void testTransitiveChain()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);
        substitution.put(2, 3);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);
        assertThat(substitution.getOrIdentity(2)).isEqualTo(3);
        assertThat(substitution.getOrIdentity(3)).isEqualTo(3);
    }

    @Test
    void testLongChain()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);
        substitution.put(2, 3);
        substitution.put(3, 4);
        substitution.put(4, 5);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(5);
        assertThat(substitution.getOrIdentity(2)).isEqualTo(5);
        assertThat(substitution.getOrIdentity(3)).isEqualTo(5);
        assertThat(substitution.getOrIdentity(4)).isEqualTo(5);
    }

    @Test
    void testIndependentChains()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);
        substitution.put(2, 3);
        substitution.put(10, 20);
        substitution.put(20, 30);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);
        assertThat(substitution.getOrIdentity(10)).isEqualTo(30);
    }

    @Test
    void testMultipleKeysToSameTarget()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 3);
        substitution.put(2, 3);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);
        assertThat(substitution.getOrIdentity(2)).isEqualTo(3);
    }

    @Test
    void testRepeatedLookupIsStable()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);
        substitution.put(2, 3);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);
        // Second lookup exercises the path-compression branch where intermediate nodes already point to the root.
        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);
    }

    @Test
    void testChainExtendedAfterLookup()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);
        substitution.put(2, 3);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(3);

        substitution.put(3, 4);

        assertThat(substitution.getOrIdentity(1)).isEqualTo(4);
        assertThat(substitution.getOrIdentity(2)).isEqualTo(4);
        assertThat(substitution.getOrIdentity(3)).isEqualTo(4);
    }

    @Test
    void testDuplicatePutRejected()
    {
        var substitution = new Substitution<Integer>();
        substitution.put(1, 2);

        assertThatThrownBy(() -> substitution.put(1, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Mapping for 1 already exists");
    }

    @Test
    void testNull()
    {
        var substitution = new Substitution<Integer>();

        assertThatThrownBy(() -> substitution.put(null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("from is null");

        assertThatThrownBy(() -> substitution.put(1, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("to is null");

        assertThatThrownBy(() -> substitution.getOrIdentity(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("from is null");
    }
}
