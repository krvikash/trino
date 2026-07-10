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
package io.trino.plugin.hive.metastore.unity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.List;

public class StagedCommitsInfo
{
    private final List<StagedCommit> commits;
    private final Long latestTableVersion;

    @JsonCreator
    public StagedCommitsInfo(
            @JsonProperty("commits") List<StagedCommit> commits,
            @JsonProperty("latest_table_version") Long latestTableVersion)
    {
        this.commits = commits;
        this.latestTableVersion = latestTableVersion;
    }

    public List<StagedCommit> getCommits()
    {
        if (commits == null || commits.isEmpty()) {
            return commits;
        }
        // The commits returned by the Unity Catalog API are not guaranteed to be sorted by version.
        // Sort them by version to ensure consistent ordering as the callers expect.
        return commits.stream().sorted(Comparator.comparing(StagedCommit::version)).toList();
    }

    public Long getLatestTableVersion()
    {
        return latestTableVersion;
    }
}
