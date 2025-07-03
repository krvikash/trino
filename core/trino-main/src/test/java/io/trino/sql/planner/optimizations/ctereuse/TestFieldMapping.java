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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.ctereuse.FieldMapping.EMPTY;
import static io.trino.sql.planner.optimizations.ctereuse.FieldMapping.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFieldMapping
{
    private static final Type RELATION_ROW_TYPE = anonymousRow(BIGINT, BOOLEAN, VARCHAR);
    private static final Type RELATION_TYPE = new MultisetType(RELATION_ROW_TYPE);

    @Test
    public void testIdentityMapping()
    {
        assertThat(identity(RELATION_ROW_TYPE))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(identity(EMPTY_ROW))
                .isEqualTo(EMPTY);

        assertThatThrownBy(() -> identity(RELATION_TYPE))
                .hasMessage("expected relation row type");

        assertThatThrownBy(() -> identity(BIGINT))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testIsIdentity()
    {
        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0))
                .isIdentity(RELATION_ROW_TYPE))
                .isTrue();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                0, 0))
                .isIdentity(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0,
                3, 3))
                .isIdentity(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0))
                .isIdentity(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 1,
                1, 2,
                0, 0))
                .isIdentity(RELATION_ROW_TYPE))
                .isFalse();

        assertThatThrownBy(() -> new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0))
                .isIdentity(RELATION_TYPE))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testIsReordering()
    {
        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0))
                .isReordering(RELATION_ROW_TYPE))
                .isTrue();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                0, 0))
                .isReordering(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0,
                3, 3))
                .isReordering(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0))
                .isReordering(RELATION_ROW_TYPE))
                .isFalse();

        assertThat(new FieldMapping(ImmutableMap.of(
                2, 1,
                1, 2,
                0, 0))
                .isReordering(RELATION_ROW_TYPE))
                .isTrue();

        assertThatThrownBy(() -> new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 1,
                0, 0))
                .isReordering(RELATION_TYPE))
                .hasMessage("expected relation row type");
    }

    @Test
    public void testIsEmpty()
    {
        assertThat(new FieldMapping(ImmutableMap.of(1, 2)).isEmpty()).isFalse();
        assertThat(new FieldMapping(ImmutableMap.of()).isEmpty()).isTrue();
    }

    @Test
    public void testGet()
    {
        FieldMapping mapping = new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0));

        assertThat(mapping.get(0)).isEqualTo(0);
        assertThat(mapping.get(1)).isEqualTo(2);
        assertThat(mapping.get(2)).isEqualTo(2);
        assertThat(mapping.get(5)).isNull();
    }

    @Test
    public void testContainsKey()
    {
        FieldMapping mapping = new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0));

        assertThat(mapping.containsKey(0)).isTrue();
        assertThat(mapping.containsKey(5)).isFalse();
    }

    @Test
    public void testKeySet()
    {
        assertThat(new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0))
                .keySet())
                .isEqualTo(ImmutableSet.of(0, 1, 2));

        assertThat(EMPTY.keySet()).isEqualTo(ImmutableSet.of());
    }

    @Test
    public void testInverse()
    {
        assertThat(new FieldMapping(ImmutableMap.of(
                0, 0,
                1, 1,
                2, 2))
                .inverse())
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 0,
                        1, 1,
                        2, 2)));

        assertThat(new FieldMapping(ImmutableMap.of(
                0, 2,
                1, 0,
                2, 1))
                .inverse())
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 1,
                        1, 2,
                        2, 0)));

        assertThat(new FieldMapping(ImmutableMap.of(0, 100)).inverse())
                .isEqualTo(new FieldMapping(ImmutableMap.of(100, 0)));

        assertThatThrownBy(() -> new FieldMapping(ImmutableMap.of(
                2, 2,
                1, 2,
                0, 0))
                .inverse())
                .hasMessage("cannot inverse mapping");
    }

    @Test
    public void testComposeWith()
    {
        assertThat(new FieldMapping(ImmutableMap.of(
                0, 2,
                1, 3))
                .composeWith(new FieldMapping(ImmutableMap.of(
                        3, 5,
                        4, 6))))
                .isEqualTo(new FieldMapping(ImmutableMap.of(1, 5)));

        assertThat(EMPTY.composeWith(new FieldMapping(ImmutableMap.of(
                3, 5,
                4, 6))))
                .isEqualTo(EMPTY);

        assertThat(new FieldMapping(ImmutableMap.of(
                0, 2,
                1, 3))
                .composeWith(EMPTY))
                .isEqualTo(EMPTY);

        assertThat(new FieldMapping(ImmutableMap.of(
                0, 2,
                1, 3))
                .composeWith(new FieldMapping(ImmutableMap.of(
                        4, 6,
                        5, 7))))
                .isEqualTo(EMPTY);

        assertThat(new FieldMapping(ImmutableMap.of(
                0, 2,
                1, 2))
                .composeWith(new FieldMapping(ImmutableMap.of(2, 3))))
                .isEqualTo(new FieldMapping(ImmutableMap.of(
                        0, 3,
                        1, 3)));
    }
}
