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

import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public record CommitRequest(
        @JsonProperty("table_id") String tableId,
        @JsonProperty("table_uri") String tableUri,
        @JsonProperty("commit_info") StagedCommit commitInfo,
        @JsonProperty("latest_backfilled_version") Long latestBackfilledVersion,
        @JsonProperty("metadata") Metadata metadata,
        @JsonProperty("protocol") Protocol protocol)
{
    public CommitRequest
    {
        requireNonNull(tableId, "tableId is null");
        requireNonNull(tableUri, "tableUri is null");
    }
}
