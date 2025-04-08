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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.trino.metadata.FunctionManager;
import io.trino.spi.type.Type;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.LambdaBytecodeGenerator.generateLambda;

public class RowExpressionCompiler
{
    // This is an arbitrary value determined through experimentation. It must be
    // low enough to ensure that the generated code does not exceed the method size limit,
    // but high enough to minimize the overhead of method invocations.
    private static final int MAX_COMPLEXITY = 1000;

    private final ClassDefinition classDefinition;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final FunctionManager functionManager;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;
    private final List<Parameter> contextArguments;  // arguments that need to be propagates to generated methods
    private final Optional<ParentMethodContext> parentMethodContext;
    private final Map<Scope, Integer> scopeComplexity = new HashMap<>();

    public RowExpressionCompiler(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            FunctionManager functionManager,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            List<Parameter> contextArguments,
            Optional<ParentMethodContext> parentMethodContext)
    {
        this.classDefinition = classDefinition;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.functionManager = functionManager;
        this.compiledLambdaMap = compiledLambdaMap;
        this.contextArguments = ImmutableList.copyOf(contextArguments);
        this.parentMethodContext = parentMethodContext;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope)
    {
        return compile(rowExpression, scope, Optional.empty());
    }

    /**
     * Unlike {@link RowExpressionCompiler#compile(RowExpression, Scope)},
     * this method does not guarantee that the expression will be inlined into the provided scope.
     * If the scope is too complex, the expression may be extracted into a separate method,
     * and only a call to that method will be inlined in the scope.
     * <p>
     * An example of an extracted method looks like this:
     * <pre>{@code
     * private Slice evaluateExpression_0(ConnectorSession session, int position) {
     *     // restore context
     *     boolean wasNull = this.__context_0;
     *
     *     // evaluate the expression
     *     Slice var10000;
     *     if (this.block_0.isNull(position)) {
     *         wasNull = true;
     *         var10000 = null;
     *     } else {
     *         var10000 = varchar_152_<invokedynamic>().getSlice(this.block_0, position);
     *     }
     *
     *     // dump context
     *     this.__context_0 = wasNull;
     *
     *     return var10000;
     * }
     * }</pre>
     * And its invocation looks like this:
     * <pre>{@code
     * // dump context
     * this.__context_0 = wasNull;
     *
     * // invoke the method
     * Slice var612 = this.evaluateExpression_0(session, position);
     *
     * // restore context
     * wasNull = this.__context_0;
     * }</pre>
     * <p>
     * This behavior may be undesirable in some cases—for example, when temporary variables
     * are used by child expressions. See {@link io.trino.sql.gen.BetweenCodeGenerator}. In such cases,
     * {@link RowExpressionCompiler#compile(RowExpression, Scope)} should be used instead.
     * <p>
     * Scope complexity is estimated based on the number of row expressions already inlined that
     * the compiler is aware of.
     * <p>
     * Additionally, if the parent method context was not provided when instantiating the compiler,
     * extraction is effectively disabled.
     */
    public BytecodeNode compileWithExtraction(RowExpression rowExpression, Scope scope)
    {
        if (parentMethodContext.isPresent() && getCurrentComplexity(scope) > MAX_COMPLEXITY) {
            return extractToMethod(rowExpression, scope, parentMethodContext.get());
        }
        return compile(rowExpression, scope);
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Class<?>> lambdaInterface)
    {
        scopeComplexity.put(scope, getCurrentComplexity(scope) + 1);
        return rowExpression.accept(new Visitor(), new Context(scope, lambdaInterface));
    }

    private int getCurrentComplexity(Scope scope)
    {
        return scopeComplexity.getOrDefault(scope, 0);
    }

    private BytecodeNode extractToMethod(RowExpression expression, Scope parentScope, ParentMethodContext parentMethodContext)
    {
        ClassScope classScope = parentMethodContext.classScope();
        Type type = expression.type();

        // Generate a method evaluating the expression
        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PRIVATE),
                "evaluateExpression_" + classScope.getNextId(),
                type(type.getJavaType()),
                contextArguments);

        Scope methodScope = methodDefinition.getScope();
        BytecodeBlock methodBody = methodDefinition.getBody();

        // Declare context variables and fields
        Map<String, FieldDefinition> variableToFieldMap = new HashMap<>();
        for (Variable contextVariable : parentMethodContext.contextVariables()) {
            FieldDefinition field = classScope.getOrCreateContextField(contextVariable.getType());
            methodScope.declareVariable(field.getType(), contextVariable.getName());
            variableToFieldMap.put(contextVariable.getName(), field);
        }

        restoreContextVariables(methodBody, methodScope, variableToFieldMap);
        methodBody.append(compile(expression, methodScope));
        dumpContextVariables(methodBody, methodScope, variableToFieldMap);
        methodBody.ret(type.getJavaType());

        // Invoke the method
        BytecodeBlock invocation = new BytecodeBlock();
        dumpContextVariables(invocation, parentScope, variableToFieldMap);
        invocation.append(parentScope.getThis().invoke(methodDefinition, contextArguments));
        restoreContextVariables(invocation, parentScope, variableToFieldMap);

        classScope.releaseAllContextFields();

        return invocation;
    }

    private static void dumpContextVariables(BytecodeBlock block, Scope scope, Map<String, FieldDefinition> variableToFieldMap)
    {
        for (Map.Entry<String, FieldDefinition> variableToField : variableToFieldMap.entrySet()) {
            Variable variable = scope.getVariable(variableToField.getKey());
            FieldDefinition field = variableToField.getValue();
            block.append(scope.getThis().setField(field, variable));
        }
    }

    private static void restoreContextVariables(BytecodeBlock block, Scope scope, Map<String, FieldDefinition> variableToFieldMap)
    {
        for (Map.Entry<String, FieldDefinition> variableToField : variableToFieldMap.entrySet()) {
            Variable variable = scope.getVariable(variableToField.getKey());
            FieldDefinition field = variableToField.getValue();
            block.append(variable.set(scope.getThis().getField(field)));
        }
    }

    private class Visitor
            implements RowExpressionVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode visitCall(CallExpression call, Context context)
        {
            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager,
                    classDefinition,
                    contextArguments);

            return generatorContext.generateFullCall(call.resolvedFunction(), call.arguments());
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialForm specialForm, Context context)
        {
            BytecodeGenerator generator = switch (specialForm.form()) {
                case IF -> new IfCodeGenerator(specialForm);
                case NULL_IF -> new NullIfCodeGenerator(specialForm);
                case SWITCH -> new SwitchCodeGenerator(specialForm);
                case BETWEEN -> new BetweenCodeGenerator(specialForm);
                case IS_NULL -> new IsNullCodeGenerator(specialForm);
                case COALESCE -> new CoalesceCodeGenerator(specialForm);
                case IN -> new InCodeGenerator(specialForm);
                case AND -> new AndCodeGenerator(specialForm);
                case OR -> new OrCodeGenerator(specialForm);
                case DEREFERENCE -> new DereferenceCodeGenerator(specialForm);
                case ROW_CONSTRUCTOR -> new RowConstructorCodeGenerator(specialForm);
                case ARRAY_CONSTRUCTOR -> new ArrayConstructorCodeGenerator(specialForm);
                case BIND -> new BindCodeGenerator(specialForm, compiledLambdaMap, context.getLambdaInterface().get());
                default -> throw new IllegalStateException("Cannot compile special form: " + specialForm.form());
            };

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager,
                    classDefinition,
                    contextArguments);

            return generator.generateExpression(generatorContext);
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression constant, Context context)
        {
            Object value = constant.value();
            Class<?> javaType = constant.type().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                return block.comment("constant null")
                        .append(context.getScope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }

            // use LDC for primitives (boolean, short, int, long, float, double)
            block.comment("constant " + constant.type().getTypeSignature());
            if (javaType == boolean.class) {
                return block.append(loadBoolean((Boolean) value));
            }
            if (javaType == long.class) {
                return block.append(loadLong((Long) value));
            }
            if (javaType == double.class) {
                return block.append(loadDouble((Double) value));
            }
            if (javaType == String.class) {
                return block.append(loadString((String) value));
            }

            // bind constant object directly into the call-site using invoke dynamic
            Binding binding = callSiteBinder.bind(value, constant.type().getJavaType());

            return new BytecodeBlock()
                    .setDescription("constant " + constant.type())
                    .comment(constant.toString())
                    .append(loadConstant(binding));
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Context context)
        {
            return fieldReferenceCompiler.visitInputReference(node, context.getScope());
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            checkState(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
            if (!context.lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class)) {
                // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
                throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager,
                    classDefinition,
                    contextArguments);

            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(lambda),
                    context.getLambdaInterface().get());
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            if (reference.name().startsWith(TEMP_PREFIX)) {
                return context.getScope().getTempVariable(reference.name().substring(TEMP_PREFIX.length()));
            }
            return fieldReferenceCompiler.visitVariableReference(reference, context.getScope());
        }
    }

    private static final String TEMP_PREFIX = "$$TEMP$$";

    public static VariableReferenceExpression createTempVariableReferenceExpression(Variable variable, Type type)
    {
        return new VariableReferenceExpression(TEMP_PREFIX + variable.getName(), type);
    }

    private static class Context
    {
        private final Scope scope;
        private final Optional<Class<?>> lambdaInterface;

        public Context(Scope scope, Optional<Class<?>> lambdaInterface)
        {
            this.scope = scope;
            this.lambdaInterface = lambdaInterface;
        }

        public Scope getScope()
        {
            return scope;
        }

        public Optional<Class<?>> getLambdaInterface()
        {
            return lambdaInterface;
        }
    }
}
