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
package io.starburst.ai.client;

import io.airlift.slice.Slice;

import java.util.List;

public interface EmbeddingModelClient
{
    List<Double> generateEmbedding(Slice sourceString);

    List<List<Float>> generateEmbeddings(List<Slice> sourceStrings);

    default Slice generateBinaryEmbedding(Slice sourceString)
    {
        throw new UnsupportedOperationException("Binary embeddings are not supported");
    }

    default List<Slice> generateBinaryEmbeddings(List<Slice> sourceString)
    {
        throw new UnsupportedOperationException("Binary embeddings are not supported");
    }
}
