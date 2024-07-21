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

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_2;
import static io.trino.tests.product.warp.utils.TestUtils.countMethodsWithAnnotation;
import static java.util.Objects.requireNonNull;

public class TestWarpSpeedAws
        extends WarpSpeedCloudTestBase
{
    private String s3TestBucket;

    @Override
    protected void setUp()
    {
        s3TestBucket = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
    }

    @Override
    protected int countTestMethods()
    {
        return countMethodsWithAnnotation(TestWarpSpeedAws.class, Test.class);
    }

    @Override
    protected String getPathForSchema(String schemaName)
    {
        return String.format("s3://%s/%s", s3TestBucket, schemaName);
    }

    @Test(groups = {WARP_SPEED_HIVE_2, PROFILE_SPECIFIC_TESTS}, dataProvider = "synth_clouds")
    public void synth_clouds(TestFormat testFormat)
            throws IOException
    {
        testResiliencyBase(testFormat);
    }
}
