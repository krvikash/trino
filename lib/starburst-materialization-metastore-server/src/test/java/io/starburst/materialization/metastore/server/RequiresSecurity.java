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
package io.starburst.materialization.metastore.server;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Test stand-in for a deployment's framework security annotation (e.g. Galaxy's
 * {@code @ResourceSecurity}). Used to verify that a concrete subclass of the abstract
 * {@code MaterializationMetastoreResource} can contribute a class-level security annotation that a
 * {@code DynamicFeature} can discover. It lives in test scope because cork does not define or
 * enforce such an annotation itself — that is the deployment's responsibility.
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface RequiresSecurity {}
