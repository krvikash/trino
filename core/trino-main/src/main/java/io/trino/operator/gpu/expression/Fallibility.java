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

/// Whether evaluating a [GpuExpression] can raise a runtime error. The constants are ordered from
/// the least to the most fallible, so the worst of two is the greater one.
public enum Fallibility
{
    /// Evaluation never raises a runtime error.
    INFALLIBLE,
    /// Evaluation may raise a runtime error, except on rows where every input column and binding is NULL.
    FALLIBLE_EXCEPT_ALL_NULLS,
    /// Evaluation may raise a runtime error on any row.
    FALLIBLE,
    /**/;

    public static Fallibility worst(Fallibility first, Fallibility second)
    {
        return first.compareTo(second) >= 0 ? first : second;
    }

    public static Fallibility best(Fallibility first, Fallibility second)
    {
        return first.compareTo(second) <= 0 ? first : second;
    }
}
