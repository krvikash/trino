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

import io.airlift.slice.Slice;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.predicate.Range;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.warp.storage.lucene.LuceneIndexer.VALUE_FIELD_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LuceneQueryUtils
{
    // See https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html
    private static final char[] REGEXP_RESERVED_CHARACTERS = {'.', '?', '+', '*', '|', '{', '}', '[', ']', '(', ')', '"', '#', '@', '&', '<', '>', '~'};

    private LuceneQueryUtils() {}

    public static Query createRangeQuery(Range range)
    {
        Slice lowValue = null;
        if (!range.isLowUnbounded()) {
            lowValue = (Slice) range.getLowBoundedValue();
        }
        Slice highValue = null;
        if (!range.isHighUnbounded()) {
            highValue = (Slice) range.getHighBoundedValue();
        }
        return createRangeQuery(lowValue, range.isLowInclusive(), highValue, range.isHighInclusive());
    }

    // null low/high value stands for an unbounded end
    public static Query createRangeQuery(Slice lowValue, boolean lowInclusive, Slice highValue, boolean highInclusive)
    {
        if (lowInclusive && highInclusive && lowValue != null && lowValue.equals(highValue)) {
            return new TermQuery(new Term(VALUE_FIELD_NAME, new BytesRef(lowValue.getBytes())));
        }
        BytesRef lowBytes = null;
        if (lowValue != null) {
            lowBytes = new BytesRef(lowValue.getBytes());
        }
        BytesRef highBytes = null;
        if (highValue != null) {
            highBytes = new BytesRef(highValue.getBytes());
        }
        return new TermRangeQuery(VALUE_FIELD_NAME, lowBytes, highBytes, lowInclusive, highInclusive);
    }

    public static Query createPrefixQuery(Slice prefix)
    {
        return new PrefixQuery(new Term(VALUE_FIELD_NAME, new String(prefix.getBytes(), UTF_8)));
    }

    public static Query createLikeQuery(Slice like)
    {
        return new RegexpQuery(new Term(VALUE_FIELD_NAME, likeToRegexp(like)));
    }

    public static Query createOrOfLikesQuery(List<Slice> likeValues)
    {
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (Slice value : likeValues) {
            Query query = createLikeQuery(value);
            queryBuilder.add(query, BooleanClause.Occur.SHOULD);
        }
        return queryBuilder.build();
    }

    public static Query createContainsQuery(Slice slice)
    {
        return new TermQuery(new Term(VALUE_FIELD_NAME, new String(slice.getBytes(), UTF_8)));
    }

    /**
     * A pattern starting with a wildcard compiles to an automaton without a literal prefix, so matching it
     * cannot seek the terms index and scans every term in the chunk, which costs more than evaluating the
     * pattern on collected values.
     */
    public static boolean hasLiteralPrefix(Slice likePattern)
    {
        if (likePattern.length() == 0) {
            return true;
        }
        byte firstCharacter = likePattern.getByte(0);
        return firstCharacter != '%' && firstCharacter != '_';
    }

    // This method is copy-pasted from our implementation in Trino
    protected static String likeToRegexp(Slice likeSlice)
    {
        String strLike = SliceUtils.serializeSlice(likeSlice);
        // TODO: This can be done more efficiently by using a state machine and iterating over characters (See io.trino.type.LikeFunctions.likePattern(String, char, boolean))
        String regexp = strLike.replaceAll(Pattern.quote("\\"), Matcher.quoteReplacement("\\\\")); // first, escape regexp's escape character
        for (char c : REGEXP_RESERVED_CHARACTERS) {
            regexp = regexp.replaceAll(Pattern.quote(String.valueOf(c)), Matcher.quoteReplacement("\\" + c));
        }
        return regexp
                .replaceAll("%", ".*")
                .replaceAll("_", ".");
    }
}
