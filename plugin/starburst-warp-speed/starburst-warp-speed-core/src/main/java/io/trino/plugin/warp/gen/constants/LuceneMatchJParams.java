
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

package io.trino.plugin.warp.gen.constants;

public enum LuceneMatchJParams
{
    LUCENE_MATCH_JPARAMS_NATIVE_COOKIE,
    LUCENE_MATCH_JPARAMS_INDEX_UNIQUE_ID,
    LUCENE_MATCH_JPARAMS_IS_VALID_INDEX,
    LUCENE_MATCH_JPARAMS_ALL_OR_NOTHING,
    LUCENE_MATCH_JPARAMS_SI_FILE_LENGTH,
    LUCENE_MATCH_JPARAMS_CFE_FILE_LENGTH,
    LUCENE_MATCH_JPARAMS_SEGMENTS_FILE_LENGTH,
    LUCENE_MATCH_JPARAMS_CFS_FILE_LENGTH,
    LUCENE_MATCH_JPARAMS_NUM_OF;

    LuceneMatchJParams()
    {
    }
}
