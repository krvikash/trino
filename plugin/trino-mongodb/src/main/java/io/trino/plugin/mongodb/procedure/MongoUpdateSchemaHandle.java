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
package io.trino.plugin.mongodb.procedure;

import io.trino.plugin.mongodb.MongoClientConfig.SamplingOrder;
import io.trino.plugin.mongodb.procedure.UpdateSchemaProcedure.UpdateMode;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record MongoUpdateSchemaHandle(int samplingCount, SamplingOrder samplingOrder, UpdateMode mode)
        implements MongoProcedureHandle
{
    public MongoUpdateSchemaHandle
    {
        checkArgument(samplingCount >= 1, "samplingCount must be at least 1");
        requireNonNull(samplingOrder, "samplingOrder is null");
        requireNonNull(mode, "mode is null");
    }
}
