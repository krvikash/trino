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
package io.trino.tests.product.openapi;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.OPENAPI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApi
        extends ProductTest
{
    @Test(groups = {OPENAPI, PROFILE_SPECIFIC_TESTS})
    public void testSelectStaticRows()
    {
        assertThat(onTrino().executeQuery("SELECT string FROM TABLE(starburst_openapi.default.static_rows())"))
                .containsOnly(
                        row("Hello World!"),
                        row("Goodbye World!"));
    }

    @Test(groups = {OPENAPI, PROFILE_SPECIFIC_TESTS})
    public void testSelectPaginatedRows()
    {
        // TODO fix dependency issue
        assertQueryFailure(() -> onTrino().executeQuery("SELECT number FROM TABLE(starburst_openapi.default.paginated_rows()) LIMIT 1"))
                .hasMessageMatching(".*\\Qjakarta.ws.rs.ext.RuntimeDelegate: org.glassfish.jersey.internal.RuntimeDelegateImpl not a subtype\\E.*");
    }
}
