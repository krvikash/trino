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
package io.trino.plugin.hive.parquet;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.parquet.SparkVersion.SPARK_3_0_0;
import static io.trino.plugin.hive.parquet.SparkVersion.SPARK_3_1_0;
import static org.assertj.core.api.Assertions.assertThat;

class TestSparkVersion
{
    @Test
    void testCreateSparkVersion()
    {
        testCreateSparkVersion("0.5.2");
        testCreateSparkVersion("0.6.2");
        testCreateSparkVersion("0.7.3");
        testCreateSparkVersion("0.8.1");
        testCreateSparkVersion("0.9.2");
        testCreateSparkVersion("1.0.2");
        testCreateSparkVersion("1.1.1");
        testCreateSparkVersion("1.2.2");
        testCreateSparkVersion("1.3.1");
        testCreateSparkVersion("1.4.1");
        testCreateSparkVersion("1.5.2");
        testCreateSparkVersion("1.6.3");
        testCreateSparkVersion("2.0.2");
        testCreateSparkVersion("2.1.3");
        testCreateSparkVersion("2.2.3");
        testCreateSparkVersion("2.3.4");
        testCreateSparkVersion("2.4.8");
        testCreateSparkVersion("3.0.3");
        testCreateSparkVersion("3.1.3");
        testCreateSparkVersion("3.2.4");
        testCreateSparkVersion("3.3.3");
        testCreateSparkVersion("3.4.4");
        testCreateSparkVersion("3.5.6");
        testCreateSparkVersion("4.0.0");

        testCreateSparkVersion("0.5");
        testCreateSparkVersion("0.6");
        testCreateSparkVersion("0.7");
        testCreateSparkVersion("0.8");
        testCreateSparkVersion("0.9");
        testCreateSparkVersion("1.0");
        testCreateSparkVersion("1.1");
        testCreateSparkVersion("1.2");
        testCreateSparkVersion("1.3");
        testCreateSparkVersion("1.4");
        testCreateSparkVersion("1.5");
        testCreateSparkVersion("1.6");
        testCreateSparkVersion("2.0");
        testCreateSparkVersion("2.1");
        testCreateSparkVersion("2.2");
        testCreateSparkVersion("2.3");
        testCreateSparkVersion("2.4");
        testCreateSparkVersion("3.0");
        testCreateSparkVersion("3.1");
        testCreateSparkVersion("3.2");
        testCreateSparkVersion("3.3");
        testCreateSparkVersion("3.4");
        testCreateSparkVersion("3.5");
        testCreateSparkVersion("4.0");

        testCreateSparkVersion("0");
        testCreateSparkVersion("1");
        testCreateSparkVersion("2");
        testCreateSparkVersion("3");

        testCreateSparkVersion("0.5.2-SNAPSHOT");
        testCreateSparkVersion("0.6-SNAPSHOT");
        testCreateSparkVersion("0.7.3-beta");
        testCreateSparkVersion("0.7-beta");
        testCreateSparkVersion("0.8.1-snapshot");
        testCreateSparkVersion("0.8-snapshot");
        testCreateSparkVersion("0.9.2-RELEASE");
        testCreateSparkVersion("0.9-RELEASE");
        testCreateSparkVersion("1.0.0-RC1");
        testCreateSparkVersion("1.0-RC1");
        testCreateSparkVersion("1-RC1");
        testCreateSparkVersion("1.1.1-M1");
        testCreateSparkVersion("1.2.2-alpha");
        testCreateSparkVersion("1.3.1-beta2");
        testCreateSparkVersion("1.3-beta2");
        testCreateSparkVersion("1.4.1-final");
        testCreateSparkVersion("1.5.2-RC3");
        testCreateSparkVersion("1.6.3-build4");
        testCreateSparkVersion("2.0.2-20220101");
        testCreateSparkVersion("2.0-20220101");
        testCreateSparkVersion("2.1.3-2021-12-31");
        testCreateSparkVersion("2.2.3-exp");
        testCreateSparkVersion("2.3.4-test");
        testCreateSparkVersion("2.4.8-dev");
        testCreateSparkVersion("3.0.3-20240229");
        testCreateSparkVersion("3.1.3-build5");
        testCreateSparkVersion("3.3.3-alpha1");
        testCreateSparkVersion("3.4.4-beta-rc");
        testCreateSparkVersion("3.4-beta");
        testCreateSparkVersion("3-beta");
        testCreateSparkVersion("3.5.6-final2");
        testCreateSparkVersion("3.5-final");
        testCreateSparkVersion("4.0.0-SNAPSHOT");
        testCreateSparkVersion("testVersion");
    }

    @Test
    void testComparisonOfSparkVersion()
    {
        assertThat(SparkVersion.of("1.0.0").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("1.0.1").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("1.1").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("1").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("1.9.9-snapshot").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2.0.0").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2.3.1").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2.4").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2.9.9-beta").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2.10.0").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2-beta").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("2-RC1").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("3.0.0-beta").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("3.0.0-SNAPSHOT").isBelow(SPARK_3_0_0)).isTrue();
        assertThat(SparkVersion.of("3.0.0-RC1").isBelow(SPARK_3_0_0)).isTrue();

        assertThat(SparkVersion.of("1.0.0").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("1.0.1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("1.1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("1.9.9-snapshot").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2.0.0").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2.3.1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2.4").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2.9.9").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2.9.9-beta").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2-beta").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("2-RC1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("3.0.1").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("3.1.0-alpha").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("3.1.0-snapshot").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("3.1.0-RC").isBelow(SPARK_3_1_0)).isTrue();

        assertThat(SparkVersion.of("3.1.0").isBelow(SPARK_3_1_0)).isFalse();
        assertThat(SparkVersion.of("3.1.1").isBelow(SPARK_3_1_0)).isFalse();
        assertThat(SparkVersion.of("3.1.1-beta").isBelow(SPARK_3_1_0)).isFalse();
        assertThat(SparkVersion.of("3.10.0").isBelow(SPARK_3_1_0)).isFalse();
        assertThat(SparkVersion.of("4.0.0").isBelow(SPARK_3_1_0)).isFalse();

        assertThat(SparkVersion.of("testVersion").isBelow(SPARK_3_1_0)).isTrue();
        assertThat(SparkVersion.of("testVersion").isBelow(SPARK_3_0_0)).isTrue();
    }

    private void testCreateSparkVersion(String version)
    {
        SparkVersion sparkVersion = SparkVersion.of(version);
        assertThat(sparkVersion).isNotNull();
        assertThat(version).isEqualTo(sparkVersion.toString());
    }
}
