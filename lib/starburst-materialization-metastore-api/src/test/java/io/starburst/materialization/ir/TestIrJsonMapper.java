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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.starburst.materialization.ir.IrJsonMapper.irJsonMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIrJsonMapper
{
    private static final JsonCodec<Sample> CODEC = new JsonCodecFactory(irJsonMapper(new JsonMapperProvider().get())).jsonCodec(Sample.class);

    @Test
    void testRoundTripKeepsExistingAnnotations()
    {
        Sample sample = new Sample(3, true, Optional.of(5), 7, "n");
        String json = CODEC.toJson(sample);

        assertThat(json).contains("\"renamed\":\"n\"");
        assertThat(CODEC.fromJson(json)).isEqualTo(sample);
    }

    @Test
    void testAbsentNonPrimitivesAreAccepted()
    {
        assertThat(CODEC.fromJson("{\"count\":3,\"flag\":true}"))
                .isEqualTo(new Sample(3, true, Optional.empty(), null, null));
    }

    @Test
    void testMissingPrimitiveIsRejected()
    {
        assertThatThrownBy(() -> CODEC.fromJson("{\"flag\":true}"))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Cannot map `null` into type `int`");
        assertThatThrownBy(() -> CODEC.fromJson("{\"count\":3}"))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Cannot map `null` into type `boolean`");
    }

    @Test
    void testNullPrimitiveIsRejected()
    {
        assertThatThrownBy(() -> CODEC.fromJson("{\"count\":null,\"flag\":true}"))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Cannot map `null` into type `int`");
    }

    public record Sample(int count, boolean flag, Optional<Integer> size, Integer boxed, @JsonProperty("renamed") String name) {}
}
