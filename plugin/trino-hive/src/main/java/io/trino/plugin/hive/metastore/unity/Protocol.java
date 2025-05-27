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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public record Protocol(
        @JsonProperty("min_reader_version") int minReaderVersion,
        @JsonProperty("min_writer_version") int minWriterVersion,
        @JsonProperty("reader_features")
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> readerFeatures,
        @JsonProperty("writer_features")
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> writerFeatures)
{
    @JsonCreator
    public Protocol
    {
        readerFeatures = ImmutableList.copyOf(readerFeatures);
        writerFeatures = ImmutableList.copyOf(writerFeatures);
    }
}
