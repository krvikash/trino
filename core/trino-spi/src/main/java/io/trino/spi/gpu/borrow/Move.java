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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Indicates that ownership of the annotated element is transferred to the receiver.
 * The receiver becomes responsible for closing/releasing the resource.
 * <p>
 * Must be used on all method parameters and return values that pass ColumnVector,
 * HostColumnVector, or GpuPage with ownership transfer.
 * <p>
 * Example:
 * <pre>
 * {@code
 * // Takes ownership of input, must close it
 * void consume(@Move ColumnVector input) {
 *     try (input) {
 *         // Use input...
 *     } // Closed here
 * }
 *
 * // Transfers ownership to caller, caller must close it
 * @Move ColumnVector createColumn() {
 *     return new ColumnVector(...);
 * }
 * }
 * </pre>
 *
 * @see Borrow
 * @see Own
 */
@Documented
@Retention(SOURCE)
@Target({PARAMETER, METHOD})
public @interface Move {}
