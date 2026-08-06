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
package org.apache.hadoop.fs;

import java.io.IOException;

// Stub for Hadoop 3.3.6.x API missing from hadoop-apache:3.3.5-3.
// OzoneFileSystem implements this interface.
// Remove once cork's hadoop-apache is upgraded to 3.3.6.x or later.
// https://github.com/apache/hadoop/blob/branch-3.4/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/SafeMode.java
public interface SafeMode
{
    default boolean setSafeMode(SafeModeAction action)
            throws IOException
    {
        return setSafeMode(action, false);
    }

    boolean setSafeMode(SafeModeAction action, boolean isChecked)
            throws IOException;
}
