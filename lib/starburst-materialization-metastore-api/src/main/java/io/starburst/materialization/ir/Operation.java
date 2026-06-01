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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * IR node in materialization's computation graph.
 **/
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Output.class, name = Output.NAME),
        @JsonSubTypes.Type(value = TableScan.class, name = TableScan.NAME),
})
public sealed interface Operation
        permits Output, TableScan
{
    /**
     * Engine-side declaration of supported IR-node versions.
     * <p>
     * A stored {@code RawMaterializationDefinition.irVersions} map is compatible iff
     * every (typeName, version) entry matches an entry in this map exactly.
     * Unknown type names and version mismatches both fail the check.
     */
    Map<String, Integer> SUPPORTED = ImmutableMap.of(
            Output.NAME, Output.VERSION,
            TableScan.NAME, TableScan.VERSION);

    int version();

    String name();
}
