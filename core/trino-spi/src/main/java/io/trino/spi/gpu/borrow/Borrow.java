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
package io.trino.spi.gpu.borrow;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.LOCAL_VARIABLE;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Indicates that the annotated element is borrowed and should not be closed by the borrower.
 * Ownership remains with the caller/lender.
 * <p>
 * Must be used on all method parameters and return values that pass ColumnVector,
 * HostColumnVector, or GpuPage by reference without transferring ownership.
 * <p>
 * Example:
 * <pre>
 * {@code
 * // Borrows input, caller retains ownership
 * ColumnVector process(@Borrow ColumnVector input) {
 *     // Do NOT call input.close() here
 *     return result;
 * }
 *
 * // Returns borrowed reference, caller must not close it
 * @Borrow ColumnVector getColumn(int index) {
 *     return columns.get(index);
 * }
 * }
 * </pre>
 *
 * @see Move
 * @see Own
 */
@Documented
@Retention(SOURCE)
@Target({FIELD, LOCAL_VARIABLE, TYPE_USE, PARAMETER, METHOD})
public @interface Borrow {}
