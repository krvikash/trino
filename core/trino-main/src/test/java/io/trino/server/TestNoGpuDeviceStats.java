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
package io.trino.server;

import io.trino.operator.torin.GpuDeviceStats;
import io.trino.operator.torin.NoGpuDeviceStats;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code io.trino.operator.torin.Test*} is excluded from non-GPU CI builds, so this consistency
 * check lives outside that package. {@link NoGpuDeviceStats} is exported to JMX under
 * {@link GpuDeviceStats}'s generated name (see {@code ServerMainModule}), so both classes must
 * expose the same {@code org.weakref.jmx}-annotated methods (with matching signatures) or the
 * JMX schema will differ depending on which node answers a {@code jmx.current} query.
 */
class TestNoGpuDeviceStats
{
    @Test
    void testValuesAreZero()
    {
        NoGpuDeviceStats stats = new NoGpuDeviceStats();
        assertThat(stats.getDeviceTotalBytes()).isZero();
        assertThat(stats.getRmmAllocatedBytes()).isZero();
        assertThat(stats.getRmmPeakAllocatedBytes()).isZero();
        assertThat(stats.getDeviceUsedBytes()).isZero();
        assertThat(stats.getRmmAllocatedDistribution().getAllTime().getCount()).isZero();
        assertThat(stats.getDeviceUsedDistribution().getAllTime().getCount()).isZero();
    }

    @Test
    void testManagedAnnotationsConsistency()
    {
        Set<String> gpuDeviceStats = inspectManagedMethods(GpuDeviceStats.class);
        // Test self-test
        assertThat(gpuDeviceStats)
                .contains(
                        """
                        @org.weakref.jmx.Managed(name="", description="")
                        public long io.trino.operator.torin.GpuDeviceStats.getDeviceTotalBytes()""")
                .contains(
                        """
                        @org.weakref.jmx.Managed(name="", description="")
                        @org.weakref.jmx.Nested()
                        public io.airlift.stats.DistributionStat io.trino.operator.torin.GpuDeviceStats.getDeviceUsedDistribution()""");

        Set<String> noGpuDeviceStats = inspectManagedMethods(NoGpuDeviceStats.class);
        assertThat(noGpuDeviceStats.stream().map(method -> method.replace("NoGpuDeviceStats", "GpuDeviceStats")).collect(toImmutableSet()))
                // contains() might produce better error message when things go wrong ...
                .containsExactlyInAnyOrderElementsOf(gpuDeviceStats)
                // ... but equality is what we really want to assert
                .isEqualTo(gpuDeviceStats);
    }

    private static Set<String> inspectManagedMethods(Class<?> clazz)
    {
        Set<String> methods = Stream.of(clazz.getMethods())
                .filter(method -> Stream.of(method.getAnnotations()).anyMatch(TestNoGpuDeviceStats::isJmxAnnotation))
                .map(method -> {
                    String annotations = Stream.of(method.getAnnotations())
                            .filter(TestNoGpuDeviceStats::isJmxAnnotation)
                            .map(Annotation::toString)
                            .sorted()
                            .collect(joining("\n", "", "\n"));
                    return annotations + method;
                })
                .collect(toImmutableSet());
        assertThat(methods).as("JMX-annotated methods on %s", clazz).isNotEmpty();
        return methods;
    }

    private static boolean isJmxAnnotation(Annotation annotation)
    {
        return annotation.annotationType().getName().startsWith("org.weakref.jmx.");
    }
}
