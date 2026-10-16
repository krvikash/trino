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
package io.trino.operator.gpu.regex;

import io.trino.operator.gpu.regex.GpuRegexTranspiler.ExtractPattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.regex.GpuRegexTranspiler.transpileForExtract;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestGpuRegexTranspiler
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testTranspileForExtract()
    {
        assertThat(transpileForExtract("a+", 0)).contains(new ExtractPattern("(a+)", 0));
        assertThat(transpileForExtract("(a)(b)", 0)).contains(new ExtractPattern("((?:ab))", 2));
        assertThat(transpileForExtract("(a)(b)", 1)).contains(new ExtractPattern("(?:(a)b)", 2));
        assertThat(transpileForExtract("(a)(b)", 2)).contains(new ExtractPattern("(?:a(b))", 2));
        assertThat(transpileForExtract("((a)b)", 1)).contains(new ExtractPattern("((?:ab))", 2));
        assertThat(transpileForExtract("((a)b)", 2)).contains(new ExtractPattern("(?:(a)b)", 2));
        assertThat(transpileForExtract("(?:x(a))(b)", 2)).contains(new ExtractPattern("(?:(?:xa)(b))", 2));
        assertThat(transpileForExtract("(a|b)(c)", 2)).contains(new ExtractPattern("(?:(?:a|b)(c))", 2));
    }

    @Test
    void testTranspileForExtractRepeatedGroupNotSelected()
    {
        assertThat(transpileForExtract("(ab)*", 0)).contains(new ExtractPattern("((?:ab)*)", 1));
        assertThat(transpileForExtract("(ab)*(c)", 2)).contains(new ExtractPattern("(?:(?:ab)*(c))", 2));
        assertThat(transpileForExtract("(a|b)+(c)", 2)).contains(new ExtractPattern("(?:(?:a|b)+(c))", 2));
        assertThat(transpileForExtract("(a+)?(c)", 2)).contains(new ExtractPattern("(?:(?:a+)?(c))", 2));
    }

    @Test
    void testTranspileForExtractMissingGroup()
    {
        assertThat(transpileForExtract("abc", 1)).isEmpty();
        assertThat(transpileForExtract("(a)(b)", 3)).isEmpty();
        assertThatThrownBy(() -> transpileForExtract("(a)", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTranspileForExtractUnsupported()
    {
        assertThat(transpileForExtract("(?<name>a)", 1)).isEmpty();
        assertThat(transpileForExtract("(a+?)", 1)).isEmpty();
        assertThat(transpileForExtract("", 0)).isEmpty();
    }

    @Test
    void testTranspileForExtractGroupReference()
    {
        for (int group = 0; group <= 2; group++) {
            assertThat(transpileForExtract("(a)(b)\\1", group)).isEmpty();
            assertThat(transpileForExtract("(a)(b)\\2", group)).isEmpty();
        }
    }

    @Test
    void testTranspileForExtractRepeatOfPossiblyEmpty()
    {
        // Left to cuDF to accept or reject
        assertThat(transpileForExtract("(a*)+", 1)).contains(new ExtractPattern("(a*)+", 1));
        assertThat(transpileForExtract("(?:a|b?)*", 0)).contains(new ExtractPattern("((?:a|b?)*)", 0));
        assertThat(transpileForExtract("(^)+", 1)).contains(new ExtractPattern("(^)+", 1));
        assertThat(transpileForExtract("(^)?", 1)).contains(new ExtractPattern("(^)?", 1));
        assertThat(transpileForExtract("(?:(?:a)*b*)?", 0)).contains(new ExtractPattern("((?:a*b*)?)", 0));
        assertThat(transpileForExtract("(a?)?", 1)).contains(new ExtractPattern("(a?)?", 1));
    }

    @Test
    void testTranspileForExtractNestedRepeat()
    {
        assertThat(transpileForExtract("(?:a+)?", 0)).contains(new ExtractPattern("((?:a+)?)", 0));
        assertThat(transpileForExtract("(?:a?)?", 0)).contains(new ExtractPattern("((?:a?)?)", 0));
        assertThat(transpileForExtract("(?:a+)+", 0)).contains(new ExtractPattern("((?:a+)+)", 0));
        assertThat(transpileForExtract("(a+)+", 0)).contains(new ExtractPattern("((?:a+)+)", 1));
    }
}
