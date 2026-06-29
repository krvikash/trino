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

import com.google.inject.Inject;

/**
 * Test stand-in for a deployment's secured resource: a concrete {@link MaterializationMetastoreResource}
 * subclass that carries a class-level security annotation ({@link RequiresSecurity}). It verifies that a
 * {@code DynamicFeature} can discover the annotation on the registered subclass and enforce authentication.
 */
@RequiresSecurity
public class SecuredMaterializationMetastoreResource
        extends MaterializationMetastoreResource
{
    @Inject
    public SecuredMaterializationMetastoreResource(DbRawMaterializationMetastore metastore)
    {
        super(metastore);
    }
}
