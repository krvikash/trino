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
package io.trino.sql.dialect.trino.operationmetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.newir.Operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.util.Objects.requireNonNull;

public record TrinoAttributeMetadata<T>(TrinoAttributeSignature<T> trinoAttributeSignature, Function<String, T> parseMethod, Function<T, String> printMethod)
{
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    private static final JsonCodec<ResolvedFunction> RESOLVED_FUNCTION_CODEC = JSON_CODEC_FACTORY.jsonCodec(ResolvedFunction.class);

    public TrinoAttributeMetadata
    {
        requireNonNull(trinoAttributeSignature, "attributeSignature is null");
        requireNonNull(parseMethod, "parseMethod is null");
        requireNonNull(printMethod, "printMethod is null");
    }

    public T parse(String string)
    {
        return parseMethod.apply(string);
    }

    @SuppressWarnings("unchecked")
    public String print(Object attribute)
    {
        return printMethod.apply((T) attribute);
    }

    public static TrinoAttributeMetadata<Boolean> internalBooleanAttributeMetadata(String prefix, String name)
    {
        return booleanAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<Boolean> booleanAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), Boolean::valueOf, Object::toString);
    }

    public static <T extends Enum<T>> TrinoAttributeMetadata<T> internalEnumAttributeMetadata(String prefix, String name, Class<T> type)
    {
        return enumAttributeMetadata(prefixedName(prefix, name), false, type);
    }

    public static <T extends Enum<T>> TrinoAttributeMetadata<T> enumAttributeMetadata(String name, boolean external, Class<T> type)
    {
        return new TrinoAttributeMetadata<>(
                new TrinoAttributeSignature<>(name, external),
                string -> {
                    try {
                        return Enum.valueOf(type, string);
                    }
                    catch (IllegalArgumentException e) {
                        throw new TrinoException(IR_ERROR, "cannot parse attribute: " + string);
                    }
                },
                Enum::name);
    }

    public static TrinoAttributeMetadata<Integer> internalIntegerAttributeMetadata(String prefix, String name)
    {
        return integerAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<Integer> integerAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), Integer::valueOf, Object::toString);
    }

    public static TrinoAttributeMetadata<List<Integer>> internalIntegerListAttributeMetadata(String prefix, String name)
    {
        return integerListAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<List<Integer>> integerListAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), IntegerList::parse, IntegerList::print);
    }

    public static TrinoAttributeMetadata<List<List<Integer>>> internalIntegerListListAttributeMetadata(String prefix, String name)
    {
        return integerListListAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<List<List<Integer>>> integerListListAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), IntegerListList::parse, IntegerListList::print);
    }

    public static TrinoAttributeMetadata<Long> internalLongAttributeMetadata(String prefix, String name)
    {
        return longAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<Long> longAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), Long::valueOf, Object::toString);
    }

    public static <T> TrinoAttributeMetadata<T> internalObjectAttributeMetadata(String prefix, String name, JsonCodec<T> codec)
    {
        return objectAttributeMetadata(prefixedName(prefix, name), false, codec);
    }

    public static <T> TrinoAttributeMetadata<T> objectAttributeMetadata(String name, boolean external, JsonCodec<T> codec)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), codec::fromJson, codec::toJson);
    }

    public static <T> TrinoAttributeMetadata<T> internalObjectAttributeMetadata(String prefix, String name, Function<String, T> parseMethod, Function<T, String> printMethod)
    {
        return objectAttributeMetadata(prefixedName(prefix, name), false, parseMethod, printMethod);
    }

    public static <T> TrinoAttributeMetadata<T> objectAttributeMetadata(String name, boolean external, Function<String, T> parseMethod, Function<T, String> printMethod)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), parseMethod, printMethod);
    }

    public static TrinoAttributeMetadata<ResolvedFunction> internalResolvedFunctionAttributeMetadata(String prefix, String name)
    {
        return internalObjectAttributeMetadata(prefix, name, RESOLVED_FUNCTION_CODEC);
    }

    public static TrinoAttributeMetadata<SortOrderList> internalSortOrderListAttributeMetadata(String prefix, String name)
    {
        return sortOrderListAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<SortOrderList> sortOrderListAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), SortOrderList::parse, SortOrderList::print);
    }

    public static TrinoAttributeMetadata<List<String>> internalStringListAttributeMetadata(String prefix, String name)
    {
        return stringListAttributeMetadata(prefixedName(prefix, name), false);
    }

    public static TrinoAttributeMetadata<List<String>> stringListAttributeMetadata(String name, boolean external)
    {
        return new TrinoAttributeMetadata<>(new TrinoAttributeSignature<>(name, external), StringList::parse, StringList::print);
    }

    public static String prefixedName(String prefix, String name)
    {
        return prefix + ':' + name;
    }

    public record TrinoAttributeSignature<T>(String name, boolean external)
    {
        public TrinoAttributeSignature
        {
            requireNonNull(name, "name is null");
            if (name.isEmpty()) {
                throw new TrinoException(IR_ERROR, "attribute name is empty");
            }
        }

        @SuppressWarnings("unchecked")
        public T getAttribute(Map<Operation.AttributeKey, Object> map)
        {
            return (T) map.get(new Operation.AttributeKey(TRINO, name));
        }

        public void putAttribute(ImmutableMap.Builder<Operation.AttributeKey, Object> builder, T attribute)
        {
            builder.put(new Operation.AttributeKey(TRINO, name), attribute);
        }

        @SuppressWarnings("unchecked")
        public T putAttribute(Map<Operation.AttributeKey, Object> map, T attribute)
        {
            return (T) map.put(new Operation.AttributeKey(TRINO, name), attribute);
        }

        public Map<Operation.AttributeKey, Object> asMap(T attribute)
        {
            return ImmutableMap.of(new Operation.AttributeKey(TRINO, name), attribute);
        }
    }

    public record IntegerList()
    {
        private static final JsonCodec<List<Integer>> INTEGER_LIST_CODEC = JSON_CODEC_FACTORY.listJsonCodec(Integer.class);

        public static List<Integer> parse(String string)
        {
            return ImmutableList.copyOf(INTEGER_LIST_CODEC.fromJson(string));
        }

        public static String print(List<Integer> integerList)
        {
            return INTEGER_LIST_CODEC.toJson(integerList);
        }
    }

    public record IntegerListList()
    {
        private static final JsonCodec<List<List<Integer>>> INTEGER_LIST_LIST_CODEC = JSON_CODEC_FACTORY.listJsonCodec(JSON_CODEC_FACTORY.listJsonCodec(Integer.class));

        public static List<List<Integer>> parse(String string)
        {
            return ImmutableList.copyOf(INTEGER_LIST_LIST_CODEC.fromJson(string));
        }

        public static String print(List<List<Integer>> integerListList)
        {
            return INTEGER_LIST_LIST_CODEC.toJson(integerListList);
        }
    }

    public record SortOrderList(List<SortOrder> sortOrders)
    {
        private static final JsonCodec<List<SortOrder>> SORT_ORDERS_CODEC = JSON_CODEC_FACTORY.listJsonCodec(JSON_CODEC_FACTORY.jsonCodec(SortOrder.class));

        public SortOrderList(List<SortOrder> sortOrders)
        {
            requireNonNull(sortOrders, "sortOrders is null");
            if (sortOrders.isEmpty()) {
                throw new TrinoException(IR_ERROR, "sortOrders is empty");
            }
            this.sortOrders = ImmutableList.copyOf(sortOrders);
        }

        public static SortOrderList parse(String string)
        {
            return new SortOrderList(SORT_ORDERS_CODEC.fromJson(string));
        }

        public static String print(SortOrderList sortOrderList)
        {
            return SORT_ORDERS_CODEC.toJson(sortOrderList.sortOrders());
        }
    }

    public record StringList()
    {
        private static final JsonCodec<List<String>> STRING_LIST_CODEC = JSON_CODEC_FACTORY.listJsonCodec(String.class);

        public static List<String> parse(String string)
        {
            return ImmutableList.copyOf(STRING_LIST_CODEC.fromJson(string));
        }

        public static String print(List<String> stringList)
        {
            return STRING_LIST_CODEC.toJson(stringList);
        }
    }
}
