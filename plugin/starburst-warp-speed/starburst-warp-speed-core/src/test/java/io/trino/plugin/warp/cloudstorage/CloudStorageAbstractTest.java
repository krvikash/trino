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
package io.trino.plugin.warp.cloudstorage;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class CloudStorageAbstractTest
{
    public static final int MB = 1048576;

    protected CloudStorage cloudStorage;

    protected File createTempFile(String fileName, int size)
            throws IOException
    {
        File tempFile = Files.createTempFile(fileName, ".temp").toFile();
        Writer writer = Files.newBufferedWriter(tempFile.toPath(), StandardCharsets.UTF_8);
        int count = size / tempFile.getName().length();

        for (int i = 0; i < count + 1; i++) {
            writer.write(tempFile.getName() + " ");
        }
        writer.close();
        return tempFile;
    }

    protected abstract String getBucket();

    protected abstract String getEmptyBucket();

    protected abstract String getNotExistBucket();

    @Test
    void test_uploadFile()
            throws IOException
    {
        File localFile = createTempFile("CloudStorageUploadFile", 25 * MB);
        Location source = Location.of(localFile.getPath());
        Location target = Location.of(getBucket() + "dir/path/key/" + localFile.getName());

        cloudStorage.uploadFile(source, target);

        TrinoInputFile inputFile = cloudStorage.newInputFile(target);
        assertThat(inputFile.exists()).isTrue();
        assertThat(inputFile.length()).isEqualTo(localFile.length());

        boolean _ = localFile.delete();
        cloudStorage.deleteFile(target);
    }

    @Test
    void test_downloadFile()
            throws IOException
    {
        File localFile = createTempFile("CloudStorageDownloadFile", 25 * MB);
        Location local = Location.of(localFile.getPath());
        Location source = Location.of(getBucket() + "dir/path/key/" + localFile.getName());
        cloudStorage.uploadFile(local, source);

        Location target = Location.of(localFile.getParent() + "/CloudStorageDownloadedFile.temp");
        cloudStorage.downloadFile(source, target);

        File targetFile = new File(target.toString());
        assertThat(targetFile).exists().hasSize(localFile.length());

        localFile.delete();
        targetFile.delete();
        cloudStorage.deleteFile(source);
    }

    @Test
    void test_copyFile()
            throws IOException
    {
        File localFile = createTempFile("CloudStorageCopyFile", 25 * MB);
        Location local = Location.of(localFile.getPath());
        Location source = Location.of(getBucket() + "dir/path/key/" + localFile.getName());
        cloudStorage.uploadFile(local, source);

        Location destination = source.sibling("CloudStorageCopiedFile.temp");
        cloudStorage.copyFile(source, destination);

        TrinoInputFile sourceFile = cloudStorage.newInputFile(source);
        assertThat(sourceFile.exists()).isTrue();
        assertThat(sourceFile.length()).isEqualTo(localFile.length());

        TrinoInputFile destinationFile = cloudStorage.newInputFile(destination);
        assertThat(destinationFile.exists()).isTrue();
        assertThat(destinationFile.length()).isEqualTo(sourceFile.length());

        boolean _ = localFile.delete();
        cloudStorage.deleteFile(source);
        cloudStorage.deleteFile(destination);
    }

    @Test
    void test_copyFileReplaceTail()
            throws IOException
    {
        File localFile = createTempFile("CloudStorageCopyReplaceTailFile", 18 * MB);
        Location local = Location.of(localFile.getPath());
        Location source = Location.of(getBucket() + "dir/path/key/" + localFile.getName());
        cloudStorage.uploadFile(local, source);

        Location destination = source.sibling("CloudStorageCopiedReplacedTailFile.temp");
        long position = 17 * MB;
        byte[] buffer = new byte[2 * MB];
        new Random().nextBytes(buffer);

        cloudStorage.copyFileReplaceTail(source, destination, new CloudObjectMetadata(), position, buffer);

        TrinoInputFile sourceFile = cloudStorage.newInputFile(source);
        assertThat(sourceFile.exists()).isTrue();
        assertThat(sourceFile.length()).isEqualTo(localFile.length());

        TrinoInputFile destinationFile = cloudStorage.newInputFile(destination);
        assertThat(destinationFile.exists()).isTrue();
        assertThat(destinationFile.length()).isEqualTo(19 * MB);

        boolean _ = localFile.delete();
        cloudStorage.deleteFile(source);
        cloudStorage.deleteFile(destination);
    }

    @Test
    void test_renameFile()
            throws IOException
    {
        File localFile = createTempFile("CloudStorageRenameFile", 15 * MB);
        Location local = Location.of(localFile.getPath());
        Location source = Location.of(getBucket() + "dir/path/key/" + localFile.getName());
        cloudStorage.uploadFile(local, source);

        Location target = source.sibling("CloudStorageRenamedFile.temp");
        cloudStorage.renameFile(source, target);

        TrinoInputFile sourceFile = cloudStorage.newInputFile(source);
        assertThat(sourceFile.exists()).isFalse();

        TrinoInputFile targetFile = cloudStorage.newInputFile(target);
        assertThat(targetFile.exists()).isTrue();
        assertThat(targetFile.length()).isEqualTo(localFile.length());

        boolean _ = localFile.delete();
        cloudStorage.deleteFile(target);
    }

    @Test
    void test_listFiles()
            throws IOException
    {
        Location location = Location.of(getBucket());
        FileIterator fileIterator = cloudStorage.listFiles(location);
        assertThat(fileIterator.hasNext()).isTrue();

        location = Location.of(getEmptyBucket());
        fileIterator = cloudStorage.listFiles(location);
        assertThat(fileIterator.hasNext()).isFalse();
    }

    @Test
    void test_directoryExists()
            throws IOException
    {
        Location location = Location.of(getBucket());
        Optional<Boolean> optional = cloudStorage.directoryExists(location);
        assertThat(optional).hasValue(true);

        location = Location.of(getEmptyBucket());
        optional = cloudStorage.directoryExists(location);
        assertThat(optional).hasValue(true);

        location = Location.of(getNotExistBucket());
        optional = cloudStorage.directoryExists(location);
        assertThat(optional).hasValue(false);
    }

    @Test
    void test_listDirectories()
            throws IOException
    {
        Location location = Location.of(getBucket());
        Set<Location> locations = cloudStorage.listDirectories(location);
        assertThat(locations).isNotNull();

        location = Location.of(getEmptyBucket());
        locations = cloudStorage.listDirectories(location);
        assertThat(locations).isEmpty();
    }
}
