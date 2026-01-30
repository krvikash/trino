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

import org.apache.maven.artifact.versioning.ComparableVersion;

class SparkVersion
{
    static final SparkVersion SPARK_3_0_0 = new SparkVersion("3.0.0");
    static final SparkVersion SPARK_3_1_0 = new SparkVersion("3.1.0");

    private final ComparableVersion version;

    private SparkVersion(String version)
    {
        this.version = new ComparableVersion(version);
    }

    static SparkVersion of(String version)
    {
        return new SparkVersion(version);
    }

    boolean isBelow(SparkVersion other)
    {
        return version.compareTo(other.version) < 0;
    }

    @Override
    public String toString()
    {
        return version.toString();
    }
}
