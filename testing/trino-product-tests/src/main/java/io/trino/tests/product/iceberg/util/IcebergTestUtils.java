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
package io.trino.tests.product.iceberg.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public final class IcebergTestUtils
{
    private static final String HDFS_URI = "hdfs://hadoop-master:9000";

    private IcebergTestUtils() {}

    public static String getTableLocation(String tableName, boolean withURI)
    {
        String regex = format(".*location = '%s(.*?)'.*", withURI ? "" : HDFS_URI);
        Pattern locationPattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) onTrino().executeQuery("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    public static String getTableLocation(String tableName)
    {
        return getTableLocation(tableName, false);
    }
}
