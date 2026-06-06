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
package io.trino.plugin.hive;

import io.trino.filesystem.s3.CustomCredentialProviders;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHive3OnDataLakeWithAwsCustomCredentialProvider
        extends BaseTestHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-hive-insert-overwrite-" + randomNameSuffix();

    public TestHive3OnDataLakeWithAwsCustomCredentialProvider()
    {
        super(BUCKET_NAME, new Hive3FlociDataLake(BUCKET_NAME, HiveHadoop.HIVE3_IMAGE), hiveFlociDataLake -> S3HiveQueryRunner.builderWithAwsCustomCredentialProvider(hiveFlociDataLake, CustomCredentialProviders.FlociAwsCredentialsProvider.class.getName()));
    }
}
