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
package io.trino.tests.product.warp;

import io.trino.tests.product.warp.utils.TestFormat;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_AZURE_HIVE;
import static io.trino.tests.product.warp.utils.TestUtils.countMethodsWithAnnotation;
import static java.util.Objects.requireNonNull;

public class TestWarpSpeedAzure
        extends WarpSpeedCloudTestBase
{
    private String abfsContainer;
    private String abfsAccount;

    public TestWarpSpeedAzure()
    {
        super();
    }

    @Override
    protected void setUp()
    {
        if (abfsContainer == null) {
            abfsContainer = requireNonNull(System.getenv("ABFS_CONTAINER"), "Environment variable not set: ABFS_CONTAINER");
        }
        if (abfsAccount == null) {
            abfsAccount = requireNonNull(System.getenv("ABFS_ACCOUNT"), "Environment variable not set: ABFS_ACCOUNT");
        }
    }

    @Override
    protected int countTestMethods()
    {
        return countMethodsWithAnnotation(TestWarpSpeedAzure.class, Test.class);
    }

    @Override
    protected String getPathForSchema(String schemaName)
    {
        return String.format("abfs://%s@%s.dfs.core.windows.net/%s", abfsContainer, abfsAccount, schemaName);
    }

    @Test(groups = {WARP_SPEED_AZURE_HIVE, PROFILE_SPECIFIC_TESTS}, dataProvider = "synth_clouds")
    public void synth_clouds(TestFormat testFormat)
            throws IOException
    {
        // ToDo: disable export until have permissions
        Map<String, Object> sessionProperties = new HashMap<>(testFormat.session_properties());
        sessionProperties.replace("enable_import_export", false);
        TestFormat azureTestFormat = TestFormat.builder(testFormat).sessionProperties(sessionProperties).build();

        testResiliencyBase(azureTestFormat);
    }
}
