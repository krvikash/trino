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
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.IOException;

public class AzuriteHierarchicalNamespaceChecker
        implements AzureHierarchicalNamespaceChecker
{
    @Override
    public boolean isHierarchicalNamespaceEnabled(AzureLocation location, BlobContainerClient blobContainerClient)
            throws IOException
    {
        try {
            BlockBlobClient blockBlobClient = blobContainerClient
                    .getBlobClient("/")
                    .getBlockBlobClient();
            return blockBlobClient.exists();
        }
        catch (RuntimeException e) {
            throw new IOException("Checking whether hierarchical namespace is enabled for the location %s failed".formatted(location), e);
        }
    }
}
