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
package io.trino.plugin.warp.storage.lucene;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.likeToRegexp;
import static org.assertj.core.api.Assertions.assertThat;

class LuceneQueryUtilsTest
{
    // This method is copy-pasted from our implementation in Trino
    @Test
    public void testLikeToRegexp()
    {
        assertThat(likeToRegexp(Slices.utf8Slice("a_b_c"))).isEqualTo("a.b.c");
        assertThat(likeToRegexp(Slices.utf8Slice("a%b%c"))).isEqualTo("a.*b.*c");
        assertThat(likeToRegexp(Slices.utf8Slice("a%b_c"))).isEqualTo("a.*b.c");
        assertThat(likeToRegexp(Slices.utf8Slice("a[b"))).isEqualTo("a\\[b");
        assertThat(likeToRegexp(Slices.utf8Slice("a_\\b"))).isEqualTo("a.\\\\b");
    }
}
