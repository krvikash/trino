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

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;

/**
 * Contains context for generating entire row expression evaluation code that can be split into multiple classes.
 * When the main class starts to be too large (constant pool size), the new methods are extracted into a new, class (chunk class).
 * The process repeats for chunk classes as well.
 * Each chunk class contains fields needed to support the evaluation:
 * - Reference to the main object. This is needed for lambda expression are those are generated only in the main class but can be called from any chunk class.
 * - References to the Block fields from the main PageProjectionWork class, used as a source of data for the projections.
 * - References to the chunk objects that contain generated methods the current class needs to call.
 * - Context fields used to pass temporary state between method calls.
 * <p>
 * Sample chunk class:
 * <pre>
 *      public final class RowExpressionEvaluationChunk_4_20250901_132703_80 {
 *          public boolean __context_0;
 *          public boolean __context_1;
 *          private final PageProjectionWork_20250901_132703_71 __main;
 *          private final Block block_0;
 *          private final RowExpressionEvaluationChunk_5_20250901_132703_82 __rowExpressionChunk_6;
 *          private final RowExpressionEvaluationChunk_6_20250901_132703_84 __rowExpressionChunk_8;
 *
 *          public RowExpressionEvaluationChunk_4_20250901_132703_80(PageProjectionWork_20250901_132703_71 __main) {
 *              this.__main = __main;
 *              this.block_0 = __main.block_0;
 *              this.__rowExpressionChunk_6 = new RowExpressionEvaluationChunk_5_20250901_132703_82(this.__main);
 *              this.__rowExpressionChunk_8 = new RowExpressionEvaluationChunk_6_20250901_132703_84(this.__main);
 *          }
 *
 *         public long evaluateExpression_1(ConnectorSession session, int position) {
 *             boolean wasNull = this.__context_0;
 *             Block var10000 = this.block_0;
 *             wasNull = this.block_0.isNull(position);
 *             ...
 *             long var43;
 *             ...
 *             this.__rowExpressionChunk_6.__context_0 = wasNull;
 *             var43 = this.__rowExpressionChunk_6.evaluateExpression_0(session, position);
 *             wasNull = this.__rowExpressionChunk_6.__context_0;
 *             ...
 *             return var43;
 *         }
 *     }
 *  </pre>
 */
public class RowExpressionGenerationContext
{
    private final int maxMethodsPerClass;
    private final ClassScope mainClass;
    private final List<Variable> fieldsCopiedFromMain;
    private final Map<ClassDefinition, ChunkClass> chunkClasses = new HashMap<>();
    private ClassScope currentChunkClass;
    private int nextChunkFieldId;

    public RowExpressionGenerationContext(int maxMethodsPerClass, ClassDefinition mainClass, CachedInstanceBinder cachedInstanceBinder, List<Variable> fieldsCopiedFromMain)
    {
        this.maxMethodsPerClass = maxMethodsPerClass;
        this.mainClass = new ClassScope(mainClass, cachedInstanceBinder);
        this.fieldsCopiedFromMain = requireNonNull(fieldsCopiedFromMain, "fieldsCopiedFromMain is null");
        this.currentChunkClass = this.mainClass;
        this.chunkClasses.put(mainClass, new ChunkClass(this.mainClass));
    }

    public ClassScope getCurrentChunkClass()
    {
        if (currentChunkClass.classDefinition().getMethods().size() >= maxMethodsPerClass) {
            ClassDefinition classDefinition = new ClassDefinition(
                    a(PUBLIC, FINAL),
                    makeClassName("RowExpressionEvaluationChunk_" + chunkClasses.size()),
                    type(Object.class));
            currentChunkClass = new ClassScope(classDefinition, new CachedInstanceBinder(classDefinition, mainClass.cachedInstanceBinder().getCallSiteBinder()));
            chunkClasses.put(classDefinition, new ChunkClass(currentChunkClass));
        }
        return currentChunkClass;
    }

    public String getChunkField(ClassDefinition sourceClass, ClassDefinition targetClass)
    {
        return chunkClasses.get(sourceClass).chunkFields.computeIfAbsent(targetClass, _ -> "__rowExpressionChunk_" + nextChunkFieldId++);
    }

    // defineClasses needs to be called after all classes are generated (after generateChunkClasses),
    // because DynamicClassLoader contains immutable callSiteBindings that will not see further changes.
    public <T> Class<? extends T> defineClasses(Class<T> mainClassSuperType, DynamicClassLoader classLoader, List<ClassDefinition> classes)
    {
        Map<String, Class<?>> definedClasses = classGenerator(classLoader)
                .defineClasses(classes);
        Class<?> mainClazz = definedClasses.get(mainClass.classDefinition().getType().getJavaClassName());
        return mainClazz.asSubclass(mainClassSuperType);
    }

    public List<ClassDefinition> generateChunkClasses()
    {
        ImmutableList.Builder<ClassDefinition> classes = ImmutableList.builder();
        for (ChunkClass chunkClass : chunkClasses.values()) {
            ClassDefinition classDefinition = chunkClass.classScope().classDefinition();
            if (!chunkClass.classScope().equals(mainClass)) {
                FieldDefinition mainField = classDefinition.declareField(a(PRIVATE, FINAL), "__main", mainClass.classDefinition().getType());
                for (Variable field : fieldsCopiedFromMain) {
                    classDefinition.declareField(a(PRIVATE, FINAL), field.getName(), field.getType());
                }

                generateConstructor(chunkClass, classDefinition, mainField);
            }

            classes.add(classDefinition);
        }
        return classes.build();
    }

    private void generateConstructor(ChunkClass chunkClass, ClassDefinition classDefinition, FieldDefinition mainField)
    {
        Parameter mainConstructorArg = arg("__main", mainClass.classDefinition().getType());
        MethodDefinition constructor = classDefinition.declareConstructor(EnumSet.of(PUBLIC), ImmutableList.of(mainConstructorArg));
        BytecodeBlock body = constructor.getBody();
        body.comment("super();")
                .append(constructor.getThis())
                .invokeConstructor(Object.class);
        body.append(constructor.getThis().setField(mainField, mainConstructorArg));
        // copy the fields from the main object, as passing it through the constructor may cross the max number of method parameters (256)
        for (Variable field : fieldsCopiedFromMain) {
            constructor.getBody().append(constructor.getThis().setField(field.getName(), mainConstructorArg.getField(mainClass.classDefinition().getType(), field.getName(), field.getType())));
        }
        chunkClass.classScope().cachedInstanceBinder().generateInitializations(constructor.getThis(), body);
        initializeChunkFields(classDefinition, constructor, constructor.getThis().getField(classDefinition.getType(), "__main", mainClass.classDefinition().getType()));
        body.ret();
    }

    public void initializeChunkFields(ClassDefinition sourceClassDefinition, MethodDefinition constructor, BytecodeExpression mainReference)
    {
        // for each chunk used by the sourceClassDefinition, we need to initialize chunk fields using constructor taking the main object as a parameter
        BytecodeBlock constructorBody = constructor.getBody();
        chunkClasses.get(sourceClassDefinition).chunkFields().forEach((chunkClass, chunkFieldName) -> {
            sourceClassDefinition.declareField(a(PRIVATE, FINAL), chunkFieldName, chunkClass.getType());
            constructorBody.append(constructor.getThis().setField(
                    chunkFieldName,
                    newInstance(chunkClass.getType(), ImmutableList.of(mainReference))));
        });
    }

    public ClassScope mainClass()
    {
        return mainClass;
    }

    private record ChunkClass(ClassScope classScope, Map<ClassDefinition, String> chunkFields)
    {
        public ChunkClass(ClassScope classScope)
        {
            this(classScope, new HashMap<>());
        }

        private ChunkClass
        {
            requireNonNull(classScope, "classScope is null");
            requireNonNull(chunkFields, "chunkFields is null");
        }
    }
}
