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
package io.trino.sql.planner.exploratory;

import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage.GROUP_PARAM_PREFIX;
import static java.util.stream.Collectors.joining;

public class MemoDebugPrinter
{
    private static final String GROUP_INDENT = "    ";
    private static final String OPERATION_INDENT = "        ";
    private static final String DETAIL_INDENT = "            ";

    private MemoDebugPrinter() {}

    public static String print(Memo memo, PrintOptions printOptions)
    {
        List<Integer> printOrder = memo.topologicalOrder();
        checkState(memo.size() == printOrder.size(), "All groups must be listed");

        StringBuilder result = new StringBuilder();
        result.append("MEMO: group count ")
                .append(memo.size())
                .append("\n");

        checkState(memo.rootGroup() == printOrder.getFirst(), "Root group must be printed first");
        result.append(printRootGroupEntry(printOrder.getFirst(), memo.getGroup(printOrder.getFirst()), printOptions));
        for (int groupId : printOrder.subList(1, printOrder.size())) {
            result.append(printGroupEntry(groupId, memo.getGroup(groupId), printOptions));
        }

        return result.toString();
    }

    private static String printRootGroupEntry(int groupId, MemoGroup group, PrintOptions printOptions)
    {
        return printGroupEntry(groupId, group, "Root Group", printOptions);
    }

    private static String printGroupEntry(int groupId, MemoGroup group, PrintOptions printOptions)
    {
        return printGroupEntry(groupId, group, "Group", printOptions);
    }

    private static String printGroupEntry(int groupId, MemoGroup group, String prefix, PrintOptions printOptions)
    {
        return GROUP_INDENT + prefix + " " + groupId + " " + printGroup(group, printOptions);
    }

    public static String printGroup(MemoGroup group, PrintOptions printOptions)
    {
        StringBuilder result = new StringBuilder();
        result.append(IntStream.range(0, group.groupParameterTypes().size())
                        .mapToObj(i -> GROUP_PARAM_PREFIX + i + ": " + printOptions.formatType(group.groupParameterTypes().get(i)))
                        .collect(joining(", ", "[", "]")))
                .append("\n");

        for (int i = 0; i < group.operations().size(); i++) {
            result.append(printOperation(group.operations().get(i), printOptions));
        }

        if (!group.attributes().isEmpty()) {
            result.append(OPERATION_INDENT)
                    .append(group.attributes().entrySet().stream()
                            .map(entry -> printOptions.formatAttribute(entry.getKey(), entry.getValue()))
                            .collect(joining(", ", "{", "}")))
                    .append("\n");
        }

        return result.toString();
    }

    private static String printOperation(MemoOperation operation, PrintOptions printOptions)
    {
        StringBuilder result = new StringBuilder();

        result.append(OPERATION_INDENT)
                .append(operation.dialect())
                .append(".")
                .append(operation.operationId().name())
                .append(operation.inherentAttributes().entrySet().stream()
                        .map(entry -> printOptions.formatAttribute(entry.getKey(), entry.getValue()))
                        .collect(joining(", ", " {", "}")))
                .append("\n");

        operation.children().forEach(child -> {
            result.append(DETAIL_INDENT)
                    .append("child: ");
            if (child instanceof GroupChild(int groupId, ParameterLineage parameterLineage)) {
                result.append("group ")
                        .append(groupId)
                        .append(", passed parameters: ")
                        .append(parameterLineage.toString());
            }
            else {
                result.append(((ParameterChild) child).toString());
            }
            result.append("\n");
        });

        return result.toString();
    }
}
