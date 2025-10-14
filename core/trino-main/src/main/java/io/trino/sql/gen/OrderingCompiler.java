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
package io.trino.sql.gen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.log.Logger;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexAppender;
import io.trino.operator.PagesIndexComparator;
import io.trino.operator.PagesIndexOrdering;
import io.trino.operator.SimplePageIndexAppender;
import io.trino.operator.SimplePageWithPositionComparator;
import io.trino.operator.SimplePagesIndexComparator;
import io.trino.operator.SyntheticAddress;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

public class OrderingCompiler
{
    private static final Logger log = Logger.get(OrderingCompiler.class);

    private final NonEvictableLoadingCache<PagesIndexComparatorCacheKey, PagesIndexOrdering> pagesIndexOrderings = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(1000),
            CacheLoader.from(key -> internalCompilePagesIndexOrdering(key.getSortTypes(), key.getSortChannels(), key.getSortOrders())));

    private final NonEvictableLoadingCache<PagesIndexComparatorCacheKey, PageWithPositionComparator> pageWithPositionComparators = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(1000),
            CacheLoader.from(key -> internalCompilePageWithPositionComparator(key.getSortTypes(), key.getSortChannels(), key.getSortOrders())));

    private final NonEvictableLoadingCache<PageAppenderCacheKey, Class<? extends PagesIndexAppender>> pagesIndexAppender = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(1000),
            CacheLoader.from(key -> internalCompilePageIndexAppender(key.sortTypes())));

    private final TypeOperators typeOperators;

    @Inject
    public OrderingCompiler(TypeOperators typeOperators)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Managed
    @Nested
    public CacheStatsMBean getPagesIndexOrderingsStats()
    {
        return new CacheStatsMBean(pagesIndexOrderings);
    }

    @Managed
    @Nested
    public CacheStatsMBean getPageWithPositionsComparatorsStats()
    {
        return new CacheStatsMBean(pageWithPositionComparators);
    }

    public PagesIndexAppender compilePagesIndexAppender(List<Type> types, ObjectArrayList<Block>[] channels)
    {
        requireNonNull(types, "types is null");
        requireNonNull(channels, "channels is null");

        try {
            Class<? extends PagesIndexAppender> pagesIndexAppenderClass = pagesIndexAppender.getUnchecked(new PageAppenderCacheKey(types));
            return pagesIndexAppenderClass.getConstructor(Object[].class).newInstance((Object) channels);
        }
        catch (Throwable e) {
            log.error(e, "Error compiling appender for types %s", types);
            return new SimplePageIndexAppender(channels);
        }
    }

    public PagesIndexOrdering compilePagesIndexOrdering(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        requireNonNull(sortTypes, "sortTypes is null");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
        checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");

        return pagesIndexOrderings.getUnchecked(new PagesIndexComparatorCacheKey(sortTypes, sortChannels, sortOrders));
    }

    @VisibleForTesting
    public PagesIndexOrdering internalCompilePagesIndexOrdering(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");

        PagesIndexComparator comparator;
        try {
            Class<? extends PagesIndexComparator> pagesHashStrategyClass = compilePagesIndexComparator(sortTypes, sortChannels, sortOrders);
            comparator = pagesHashStrategyClass.getConstructor().newInstance();
        }
        catch (Throwable e) {
            log.error(e, "Error compiling comparator for channels %s with order %s", sortChannels, sortOrders);
            comparator = new SimplePagesIndexComparator(sortTypes, sortChannels, sortOrders, typeOperators);
        }

        // we may want to load a separate PagesIndexOrdering for each comparator
        return new PagesIndexOrdering(comparator);
    }

    private Class<? extends PagesIndexComparator> compilePagesIndexComparator(
            List<Type> sortTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesIndexComparator"),
                type(Object.class),
                type(PagesIndexComparator.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));
        generatePageIndexCompareTo(classDefinition, callSiteBinder, sortTypes, sortChannels, sortOrders);

        return defineClass(classDefinition, PagesIndexComparator.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generatePageIndexCompareTo(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
        checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");

        Parameter pagesIndex = arg("pagesIndex", PagesIndex.class);
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        MethodDefinition compareToMethod = classDefinition.declareMethod(a(PUBLIC), "compareTo", type(int.class), pagesIndex, leftPosition, rightPosition);
        Scope scope = compareToMethod.getScope();

        Variable valueAddresses = scope.declareVariable(LongArrayList.class, "valueAddresses");
        compareToMethod
                .getBody()
                .comment("LongArrayList valueAddresses = pagesIndex.valueAddresses")
                .append(valueAddresses.set(pagesIndex.invoke("getValueAddresses", LongArrayList.class)));

        Variable leftPageAddress = scope.declareVariable(long.class, "leftPageAddress");
        compareToMethod
                .getBody()
                .comment("long leftPageAddress = valueAddresses.getLong(leftPosition)")
                .append(leftPageAddress.set(valueAddresses.invoke("getLong", long.class, leftPosition)));

        Variable leftBlockIndex = scope.declareVariable(int.class, "leftBlockIndex");
        compareToMethod
                .getBody()
                .comment("int leftBlockIndex = decodeSliceIndex(leftPageAddress)")
                .append(leftBlockIndex.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, leftPageAddress)));

        Variable leftBlockPosition = scope.declareVariable(int.class, "leftBlockPosition");
        compareToMethod
                .getBody()
                .comment("int leftBlockPosition = decodePosition(leftPageAddress)")
                .append(leftBlockPosition.set(invokeStatic(SyntheticAddress.class, "decodePosition", int.class, leftPageAddress)));

        Variable rightPageAddress = scope.declareVariable(long.class, "rightPageAddress");
        compareToMethod
                .getBody()
                .comment("long rightPageAddress = valueAddresses.getLong(rightPosition);")
                .append(rightPageAddress.set(valueAddresses.invoke("getLong", long.class, rightPosition)));

        Variable rightBlockIndex = scope.declareVariable(int.class, "rightBlockIndex");
        compareToMethod
                .getBody()
                .comment("int rightBlockIndex = decodeSliceIndex(rightPageAddress)")
                .append(rightBlockIndex.set(invokeStatic(SyntheticAddress.class, "decodeSliceIndex", int.class, rightPageAddress)));

        Variable rightBlockPosition = scope.declareVariable(int.class, "rightBlockPosition");
        compareToMethod
                .getBody()
                .comment("int rightBlockPosition = decodePosition(rightPageAddress)")
                .append(rightBlockPosition.set(invokeStatic(SyntheticAddress.class, "decodePosition", int.class, rightPageAddress)));

        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);
            Type sortType = sortTypes.get(i);
            MethodHandle compareBlockValue = getBlockPositionOrderingOperator(sortOrder, sortType);

            BytecodeBlock block = new BytecodeBlock()
                    .setDescription("compare channel " + sortChannel + " " + sortOrder);

            BytecodeExpression leftBlock = pagesIndex
                    .invoke("getChannel", ObjectArrayList.class, constantInt(sortChannel))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = pagesIndex
                    .invoke("getChannel", ObjectArrayList.class, constantInt(sortChannel))
                    .invoke("get", Object.class, rightBlockIndex)
                    .cast(Block.class);

            block.append(invokeDynamic(
                    BOOTSTRAP_METHOD,
                    ImmutableList.of(callSiteBinder.bind(compareBlockValue).getBindingId()),
                    "compareBlockValue",
                    compareBlockValue.type(),
                    leftBlock,
                    leftBlockPosition,
                    rightBlock,
                    rightBlockPosition));

            LabelNode equal = new LabelNode("equal");
            block.comment("if (compare != 0) return compare")
                    .dup()
                    .ifZeroGoto(equal)
                    .retInt()
                    .visitLabel(equal)
                    .pop(int.class);

            compareToMethod.getBody().append(block);
        }

        // values are equal
        compareToMethod.getBody()
                .push(0)
                .retInt();
    }

    private MethodHandle getBlockPositionOrderingOperator(SortOrder sortOrder, Type sortType)
    {
        return typeOperators.getOrderingOperator(sortType, sortOrder, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
    }

    public PageWithPositionComparator compilePageWithPositionComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        requireNonNull(sortTypes, "sortTypes is null");
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
        checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");

        return pageWithPositionComparators.getUnchecked(new PagesIndexComparatorCacheKey(sortTypes, sortChannels, sortOrders));
    }

    private PageWithPositionComparator internalCompilePageWithPositionComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        PageWithPositionComparator comparator;
        try {
            Class<? extends PageWithPositionComparator> pageWithPositionsComparatorClass = generatePageWithPositionComparatorClass(sortTypes, sortChannels, sortOrders);
            comparator = pageWithPositionsComparatorClass.getConstructor().newInstance();
        }
        catch (Throwable t) {
            log.error(t, "Error compiling comparator for channels %s with order %s", sortChannels, sortOrders);
            comparator = new SimplePageWithPositionComparator(sortTypes, sortChannels, sortOrders, typeOperators);
        }
        return comparator;
    }

    private Class<? extends PageWithPositionComparator> generatePageWithPositionComparatorClass(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PageWithPositionComparator"),
                type(Object.class),
                type(PageWithPositionComparator.class));

        classDefinition.declareDefaultConstructor(a(PUBLIC));

        generateMergeSortCompareTo(classDefinition, callSiteBinder, sortTypes, sortChannels, sortOrders);

        return defineClass(classDefinition, PageWithPositionComparator.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private void generateMergeSortCompareTo(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
        checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");

        Parameter leftPage = arg("leftPage", Page.class);
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        MethodDefinition compareToMethod = classDefinition.declareMethod(a(PUBLIC), "compareTo", type(int.class), leftPage, leftPosition, rightPage, rightPosition);

        for (int i = 0; i < sortChannels.size(); i++) {
            int sortChannel = sortChannels.get(i);
            SortOrder sortOrder = sortOrders.get(i);
            Type sortType = sortTypes.get(i);
            MethodHandle compareBlockValue = getBlockPositionOrderingOperator(sortOrder, sortType);

            BytecodeBlock block = new BytecodeBlock()
                    .setDescription("compare channel " + sortChannel + " " + sortOrder);

            BytecodeExpression leftBlock = leftPage
                    .invoke("getBlock", Block.class, constantInt(sortChannel));

            BytecodeExpression rightBlock = rightPage
                    .invoke("getBlock", Block.class, constantInt(sortChannel));

            block.append(invokeDynamic(
                    BOOTSTRAP_METHOD,
                    ImmutableList.of(callSiteBinder.bind(compareBlockValue).getBindingId()),
                    "compareBlockValue",
                    compareBlockValue.type(),
                    leftBlock,
                    leftPosition,
                    rightBlock,
                    rightPosition));

            LabelNode equal = new LabelNode("equal");
            block.comment("if (compare != 0) return compare")
                    .dup()
                    .ifZeroGoto(equal)
                    .retInt()
                    .visitLabel(equal)
                    .pop(int.class);

            compareToMethod.getBody().append(block);
        }

        // values are equal
        compareToMethod.getBody()
                .push(0)
                .retInt();
    }

    private Class<? extends PagesIndexAppender> internalCompilePageIndexAppender(List<Type> types)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesIndexAppender"),
                type(Object.class),
                type(PagesIndexAppender.class));

        Parameter channels = arg("channels", type(Object[].class));
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), channels);

        Variable thisVariable = constructorDefinition.getThis();

        BytecodeBlock constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        List<FieldDefinition> channelFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "channel_" + i, type(List.class, Block.class));
            channelFields.add(channelField);
        }

        constructor.comment("Set channel fields");
        //  this.channel_0 = (ObjectArrayList)channels[0];
        //  ...
        //  this.channel_n = (ObjectArrayList)channels[n];
        for (int index = 0; index < channelFields.size(); index++) {
            BytecodeExpression channel = channels.getElement(constantInt(index))
                    .cast(type(ObjectArrayList.class, Block.class));
            constructor.append(thisVariable.setField(channelFields.get(index), channel));
        }
        constructor.ret();

        generateAppendMethod(classDefinition, channelFields);

        return defineClass(classDefinition, PagesIndexAppender.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateAppendMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        MethodDefinition appendMethod = classDefinition.declareMethod(a(PUBLIC), "append", type(void.class), blockIndex, blockPosition, pageBuilder);

        Variable thisVariable = appendMethod.getThis();
        BytecodeBlock appendBody = appendMethod.getBody();

        //  Block block = (Block)this.channel_0.get(blockIndex);
        //  pageBuilder.getBlockBuilder(0).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(blockPosition));
        //  ...
        //  block = (Block)this.channel_n.get(blockIndex);
        //  pageBuilder.getBlockBuilder(n).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(blockPosition));
        Variable block = appendMethod.getScope().declareVariable(Block.class, "block");
        for (int channel = 0; channel < channelFields.size(); channel++) {
            appendBody
                    .comment("Block block = (Block)this.channel_(%s).get(blockIndex);", channel)
                    .append(block.set(thisVariable.getField(channelFields.get(channel))
                            .invoke("get", Object.class, blockIndex)
                            .cast(Block.class)));

            BytecodeExpression blockBuilderExpression = pageBuilder
                    .invoke("getBlockBuilder", BlockBuilder.class, constantInt(channel));
            appendBody
                    .comment("pageBuilder.getBlockBuilder(%s).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(blockPosition));", channel)
                    .append(blockBuilderExpression.invoke(
                            "append",
                            void.class,
                            block.invoke("getUnderlyingValueBlock", ValueBlock.class),
                            block.invoke("getUnderlyingValuePosition", int.class, blockPosition)));
        }
        appendBody.ret();
    }

    private static final class PagesIndexComparatorCacheKey
    {
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;

        private PagesIndexComparatorCacheKey(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            this.sortTypes = ImmutableList.copyOf(sortTypes);
            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrders = ImmutableList.copyOf(sortOrders);
            checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
            checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");
        }

        public List<Type> getSortTypes()
        {
            return sortTypes;
        }

        public List<Integer> getSortChannels()
        {
            return sortChannels;
        }

        public List<SortOrder> getSortOrders()
        {
            return sortOrders;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sortTypes, sortChannels, sortOrders);
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
            PagesIndexComparatorCacheKey other = (PagesIndexComparatorCacheKey) obj;
            return Objects.equals(this.sortTypes, other.sortTypes) &&
                    Objects.equals(this.sortChannels, other.sortChannels) &&
                    Objects.equals(this.sortOrders, other.sortOrders);
        }
    }

    private record PageAppenderCacheKey(List<Type> sortTypes) {}
}
