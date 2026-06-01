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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@JsonTypeName(Output.NAME)
public record Output(List<String> columnNames, List<Symbol> outputs, Operation source)
        implements Operation
{
    static final String NAME = "Output";
    static final int VERSION = 1;

    public Output
    {
        columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        requireNonNull(source, "source is null");
        checkArgument(columnNames.size() == outputs.size(), "columnNames and outputs must have the same size");
    }

    public Output withSource(Operation newSource)
    {
        return new Output(columnNames, outputs, newSource);
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public String name()
    {
        return NAME;
    }
}
