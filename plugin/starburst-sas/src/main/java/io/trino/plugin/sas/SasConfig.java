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
package io.trino.plugin.sas;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;

public class SasConfig
{
    public enum MappingType
    {
        FS,
        JSON,
    }

    private URI dataDirectory;
    private int splitCount = 1;
    private int minPagePerSplit = 2000;
    private MappingType mappingType = MappingType.FS;
    private Optional<File> jsonFile = Optional.empty();

    @NotNull
    public URI getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("sas.data-directory")
    @ConfigDescription("URI of the directory containing SAS7BDAT files (e.g. file:///data/sas or nfs://host/share)")
    public SasConfig setDataDirectory(URI dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }

    @Min(1)
    public int getSplitCount()
    {
        return splitCount;
    }

    @Config("sas.split-count")
    @ConfigDescription("Number of splits per table")
    public SasConfig setSplitCount(int splitCount)
    {
        this.splitCount = splitCount;
        return this;
    }

    @Min(1)
    public int getMinPagePerSplit()
    {
        return minPagePerSplit;
    }

    @Config("sas.min-page-per-split")
    @ConfigDescription("Minimum number of SAS file pages assigned to each split")
    public SasConfig setMinPagePerSplit(int minPagePerSplit)
    {
        this.minPagePerSplit = minPagePerSplit;
        return this;
    }

    @NotNull
    public MappingType getMappingType()
    {
        return mappingType;
    }

    @Config("sas.mapping-type")
    @ConfigDescription("Schema/table mapping strategy: FS (directory layout) or JSON (explicit mapping file)")
    public SasConfig setMappingType(MappingType mappingType)
    {
        this.mappingType = mappingType;
        return this;
    }

    public Optional<@FileExists File> getJsonFile()
    {
        return jsonFile;
    }

    @Config("sas.json-file")
    @ConfigDescription("Path to the JSON mapping file; required when mapping-type is JSON")
    public SasConfig setJsonFile(File jsonFile)
    {
        this.jsonFile = Optional.ofNullable(jsonFile);
        return this;
    }

    @AssertTrue(message = "sas.json-file is required when mapping-type is JSON")
    public boolean isJsonFileSetWhenMappingTypeIsJson()
    {
        return mappingType != MappingType.JSON || jsonFile.isPresent();
    }

    @AssertTrue(message = "sas.data-directory must use the file:// scheme")
    public boolean isDataDirectorySchemeValid()
    {
        return dataDirectory == null || "file".equalsIgnoreCase(dataDirectory.getScheme());
    }
}
