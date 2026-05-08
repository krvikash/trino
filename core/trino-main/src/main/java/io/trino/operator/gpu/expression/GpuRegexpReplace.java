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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.RegexProgram;
import ai.rapids.cudf.Scalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;

import java.util.List;

import static ai.rapids.cudf.CaptureGroups.EXTRACT;
import static ai.rapids.cudf.CaptureGroups.NON_CAPTURE;
import static java.util.Objects.requireNonNull;

public class GpuRegexpReplace
        implements GpuExpression
{
    private final GpuExpression source;
    private final String cudfPattern;
    private final String cudfReplacement;
    private final boolean hasBackreferences;

    public GpuRegexpReplace(GpuExpression source, String cudfPattern, String cudfReplacement, boolean hasBackreferences)
    {
        this.source = requireNonNull(source, "source is null");
        this.cudfPattern = requireNonNull(cudfPattern, "cudfPattern is null");
        this.cudfReplacement = requireNonNull(cudfReplacement, "cudfReplacement is null");
        this.hasBackreferences = hasBackreferences;
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns)
    {
        try (ColumnVector sourceColumn = source.evaluate(positionCount, inputColumns)) {
            if (hasBackreferences) {
                RegexProgram program = new RegexProgram(cudfPattern, EXTRACT);
                return sourceColumn.stringReplaceWithBackrefs(program, cudfReplacement);
            }
            else {
                RegexProgram program = new RegexProgram(cudfPattern, NON_CAPTURE);
                try (Scalar replacementScalar = Scalar.fromString(cudfReplacement)) {
                    return sourceColumn.replaceRegex(program, replacementScalar);
                }
            }
        }
    }
}
