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
package io.starburst.stargate.tablemaintenance.partitioned;

public enum FileType
{
    DATA,
    POSITION_DELETES,
    EQUALITY_DELETES;

    public static FileType determineFileType(String fileTypeRaw)
    {
        return switch (fileTypeRaw) {
            case "DATA" -> FileType.DATA;
            case "POSITION_DELETES" -> FileType.POSITION_DELETES;
            case "EQUALITY_DELETES" -> FileType.EQUALITY_DELETES;
            default -> throw new IllegalStateException("Unknown file type: " + fileTypeRaw);
        };
    }
}
