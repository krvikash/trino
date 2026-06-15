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
package io.starburst.stargate.tablemaintenance.partitioned;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.tablemaintenance.partitioned.Transform.TransformedPartitionColumn;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTransform
{
    private static final Map<Transform, TransformExpectation> EXPECTATIONS = ImmutableMap.<Transform, TransformExpectation>builder()
            .put(Transform.IDENTITY, new TransformExpectation(
                    "event_type",
                    "event_type",
                    "\"event_type\"",
                    "event_type"))
            .put(Transform.DAY, new TransformExpectation(
                    "event_ts",
                    "event_ts_day",
                    "date(\"event_ts\")",
                    "day(event_ts)"))
            .buildOrThrow();

    @Test
    public void testExpectationsCoverEveryTransform()
    {
        assertThat(EXPECTATIONS.keySet())
                .as("Add a TransformExpectation entry for every Transform value")
                .containsExactlyInAnyOrder(Transform.values());
    }

    @Test
    public void testToPartitionTransformedColumnName()
    {
        for (Transform transform : Transform.values()) {
            TransformExpectation expected = expectationFor(transform);
            assertThat(transform.toPartitionTransformedColumnName(expected.sourceColumn()))
                    .as("partition field name for %s", transform)
                    .isEqualTo(expected.partitionFieldName());
        }
    }

    @Test
    public void testToPartitionComparableColumnValue()
    {
        for (Transform transform : Transform.values()) {
            TransformExpectation expected = expectationFor(transform);
            assertThat(transform.toPartitionComparableColumnValue(expected.sourceColumn()))
                    .as("comparable column expression for %s", transform)
                    .isEqualTo(expected.comparableExpression());
        }
    }

    @Test
    public void testFindApplicableTransformReturnsMatchingTransform()
    {
        for (Transform transform : Transform.values()) {
            TransformExpectation expected = expectationFor(transform);
            assertThat(Transform.findApplicableTransform(expected.sourceColumn(), expected.partitionFieldName()))
                    .as("findApplicableTransform for %s", transform)
                    .contains(transform);
        }
    }

    @Test
    public void testFindApplicableTransformReturnsEmptyWhenPartitionNameMatchesNoTransform()
    {
        // Suffix that no current or plausible-future transform would emit.
        assertThat(Transform.findApplicableTransform("event_type", "event_type__no_such_transform"))
                .isEmpty();
    }

    @Test
    public void testResolveTransformedPartitionColumnHandlesEveryCanonicalSpecEntry()
    {
        for (Transform transform : Transform.values()) {
            TransformExpectation expected = expectationFor(transform);
            assertThat(Transform.resolveTransformedPartitionColumn(expected.partitionSpecEntry()))
                    .as("resolveTransformedPartitionColumn for %s", transform)
                    .contains(new TransformedPartitionColumn(expected.sourceColumn(), transform));
        }
    }

    @Test
    public void testFindApplicableTransformPicksIdentityWhenSourceNameLiterallyEqualsPartitionName()
    {
        // Column literally named "event_day" partitioned identity-style by "event_day" — must NOT
        // be mis-classified as day(event). PREFIXED_FIRST iterates DAY first; DAY produces
        // "event_day_day" which does not match, so IDENTITY wins.
        assertThat(Transform.findApplicableTransform("event_day", "event_day"))
                .contains(Transform.IDENTITY);
    }

    @Test
    public void testResolveTransformedPartitionColumnPicksIdentityForBarePartitionEntry()
    {
        // "event_day" looks like it could be day(event), but the partition spec entry is bare —
        // no "day(...)" wrapper — so it is identity-partitioned on a column named "event_day".
        assertThat(Transform.resolveTransformedPartitionColumn("event_day"))
                .contains(new TransformedPartitionColumn("event_day", Transform.IDENTITY));
    }

    private static TransformExpectation expectationFor(Transform transform)
    {
        return requireNonNull(
                EXPECTATIONS.get(transform),
                "No TransformExpectation entry for " + transform + " — add one to EXPECTATIONS");
    }

    private record TransformExpectation(
            String sourceColumn,
            String partitionFieldName,
            String comparableExpression,
            String partitionSpecEntry) {}
}
