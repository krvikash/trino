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
package io.trino.plugin.kudu.properties;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class RangeBoundValueSerializer
        extends JsonSerializer<RangeBoundValue>
{
    @Override
    public void serialize(RangeBoundValue value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        if (value == null) {
            gen.writeNull();
        }
        else {
            if (value.getValues().size() == 1) {
                writeValue(value.getValues().getFirst(), gen);
            }
            else {
                gen.writeStartArray();
                for (Object obj : value.getValues()) {
                    writeValue(obj, gen);
                }
                gen.writeEndArray();
            }
        }
    }

    private void writeValue(Object obj, JsonGenerator gen)
            throws IOException
    {
        if (obj == null) {
            throw new IllegalStateException("Unexpected null value");
        }
        if (obj instanceof String string) {
            gen.writeString(string);
        }
        else if (Number.class.isAssignableFrom(obj.getClass())) {
            if (obj instanceof Long l) {
                gen.writeNumber(l);
            }
            else if (obj instanceof Integer i) {
                gen.writeNumber(i);
            }
            else if (obj instanceof Short s) {
                gen.writeNumber(s);
            }
            else if (obj instanceof Double d) {
                gen.writeNumber(d);
            }
            else if (obj instanceof Float f) {
                gen.writeNumber(f);
            }
            else if (obj instanceof BigInteger bigInteger) {
                gen.writeNumber(bigInteger);
            }
            else if (obj instanceof BigDecimal bigDecimal) {
                gen.writeNumber(bigDecimal);
            }
            else {
                throw new IllegalStateException("Unknown number value: " + obj);
            }
        }
        else if (obj instanceof Boolean b) {
            gen.writeBoolean(b);
        }
        else if (obj instanceof byte[] byteArray) {
            gen.writeBinary(byteArray);
        }
    }
}
