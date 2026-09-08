/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static io.starburst.server.troubleshooting.configdump.ConfigDumper.generateUniqueFileName;
import static org.assertj.core.api.Assertions.assertThat;

class TestConfigDumper
{
    @Test
    void testGenerateUniqueFileName()
    {
        Set<String> usedNames = new HashSet<>();

        assertThat(generateUniqueFileName(usedNames, Path.of("/etc/access-control.properties")))
                .isEqualTo("access-control.properties");

        assertThat(generateUniqueFileName(usedNames, Path.of("/etc/other.properties")))
                .isEqualTo("other.properties");
    }

    @Test
    void testGenerateUniqueFileNameWithDuplicates()
    {
        Set<String> usedNames = new HashSet<>();

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir1/access-control.properties")))
                .isEqualTo("access-control.properties");

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir2/access-control.properties")))
                .isEqualTo("access-control_2.properties");

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir3/access-control.properties")))
                .isEqualTo("access-control_3.properties");
    }

    @Test
    void testGenerateUniqueFileNameSkipsCollision()
    {
        Set<String> usedNames = new HashSet<>();

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir1/foo.properties")))
                .isEqualTo("foo.properties");

        // Source file whose name matches the generated suffix pattern
        assertThat(generateUniqueFileName(usedNames, Path.of("/dir2/foo_2.properties")))
                .isEqualTo("foo_2.properties");

        // Duplicate of "foo.properties" — candidate "foo_2.properties" is taken, must skip to _3
        assertThat(generateUniqueFileName(usedNames, Path.of("/dir3/foo.properties")))
                .isEqualTo("foo_3.properties");
    }

    @Test
    void testGenerateUniqueFileNameWithoutExtension()
    {
        Set<String> usedNames = new HashSet<>();

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir1/Dockerfile")))
                .isEqualTo("Dockerfile");

        assertThat(generateUniqueFileName(usedNames, Path.of("/dir2/Dockerfile")))
                .isEqualTo("Dockerfile_2");
    }
}
