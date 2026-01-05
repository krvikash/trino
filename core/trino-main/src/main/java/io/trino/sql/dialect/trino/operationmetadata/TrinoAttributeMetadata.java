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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.newir.Operation;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;
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

    /**
     * Based on {@link NullableValue}, but implements safe equals and hashCode.
     */
    public static class ConstantValue
    {
        private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

        private final Type type;
        private final Object value;
        private final Optional<MethodHandle> equalOperator;
        private final Optional<MethodHandle> hashCodeOperator;

        public ConstantValue(Type type, Object value)
        {
            requireNonNull(type, "type is null");
            if (value != null && !Primitives.wrap(type.getJavaType()).isInstance(value)) {
                throw new IllegalArgumentException(format("Object '%s' does not match type %s", value, type.getJavaType()));
            }

            this.type = type;
            this.value = value;

            if (type.isComparable()) {
                this.equalOperator = Optional.of(TYPE_OPERATORS.getEqualOperator(type, simpleConvention(DEFAULT_ON_NULL, NEVER_NULL, NEVER_NULL))
                        .asType(methodType(boolean.class, Object.class, Object.class)));
                this.hashCodeOperator = Optional.of(TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL))
                        .asType(methodType(long.class, Object.class)));
            }
            else {
                this.equalOperator = Optional.empty();
                this.hashCodeOperator = Optional.empty();
            }
        }

        public static ConstantValue of(Type type, Object value)
        {
            requireNonNull(value, "value is null");
            return new ConstantValue(type, value);
        }

        public static ConstantValue asNull(Type type)
        {
            return new ConstantValue(type, null);
        }

        @JsonCreator
        @DoNotCall // For JSON deserialization only
        public static ConstantValue fromSerializable(@JsonProperty("serializable") Serializable serializable)
        {
            Type type = serializable.type();
            Block block = serializable.block();
            return new ConstantValue(type, block == null ? null : blockToNativeValue(type, block));
        }

        // Jackson serialization only
        @JsonProperty
        public Serializable getSerializable()
        {
            return new Serializable(type, value == null ? null : nativeValueToBlock(type, value));
        }

        public Type getType()
        {
            return type;
        }

        public Object getValue()
        {
            return value;
        }

        @Override
        public int hashCode()
        {
            long hash = Objects.hash(type);
            if (value != null) {
                hash = hash * 31 + valueHash();
            }
            return (int) hash;
        }

        private long valueHash()
        {
            if (hashCodeOperator.isEmpty()) {
                return 0;
            }
            try {
                return (long) hashCodeOperator.get().invokeExact(value);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ConstantValue other = (ConstantValue) obj;
            return Objects.equals(this.type, other.type)
                    && (this.value == null) == (other.value == null)
                    && (this.value == null || valueEquals(other.value));
        }

        private boolean valueEquals(Object otherValue)
        {
            if (equalOperator.isEmpty()) {
                return false;
            }
            try {
                return (boolean) equalOperator.get().invokeExact(value, otherValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
        }

        private static RuntimeException handleThrowable(Throwable throwable)
        {
            if (throwable instanceof Error error) {
                throw error;
            }
            if (throwable instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            return new RuntimeException(throwable);
        }

        @Override
        public String toString()
        {
            return "[type=" + type + ", value=" + (value == null ? "null" : type.getObjectValue(nativeValueToBlock(type, value), 0).toString()) + "]";
        }

        public record Serializable(Type type, Block block)
        {
            public Serializable
            {
                requireNonNull(type, "type is null");
            }
        }
    }
}
