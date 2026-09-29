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

/**
 * The durable, versioned IR a materialization's computation is persisted in.
 * <p>
 * A tree of {@link io.starburst.materialization.ir.IrNode}s: {@link io.starburst.materialization.ir.Operation}s
 * and the scalar {@link io.starburst.materialization.ir.Expression}s under them. Every wire-format decision a
 * reader makes is stamped as a name in {@link io.starburst.materialization.ir.IrVersions}, and a stored definition
 * is read only if this build supports every stamped name at the stamped version. Which functions and casts may be
 * persisted is decided by {@link io.starburst.materialization.ir.SupportedFunctions} and
 * {@link io.starburst.materialization.ir.SupportedCasts}. Constructors validate eagerly, so an invalid tree can be
 * neither built nor deserialized.
 * <p>
 * Symbol names are engine-generated bindings within one tree, never identities to compare against a query plan.
 * See the module README for the versioning rules, the safety model and the matching contract.
 */
package io.starburst.materialization.ir;
