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
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.type.TypeCoercion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.LambdaBytecodeGenerator.generateLambda;
import static java.util.Objects.requireNonNull;

public class ExpressionBytecodeCompiler
{
    private final ClassDefinition classDefinition;
    // lambda expressions are added into the main class, so we need to reference the main class object to invoke the method
    private final BytecodeExpression mainReference;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final BiFunction<Reference, Scope, BytecodeNode> referenceCompiler;
    private final FunctionManager functionManager;
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final TypeCoercion typeCoercion;
    private final int maxMethodComplexity;
    private final Map<Lambda, CompiledLambda> compiledLambdaMap;
    private final List<Parameter> contextArguments;  // arguments that need to be propagated to generated methods
    private final Optional<ParentMethodContext> parentMethodContext;
    private final Map<Scope, Integer> scopeComplexity = new HashMap<>();

    public ExpressionBytecodeCompiler(
            ClassDefinition classDefinition,
            BytecodeExpression mainReference,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            BiFunction<Reference, Scope, BytecodeNode> referenceCompiler,
            FunctionManager functionManager,
            Metadata metadata,
            TypeManager typeManager,
            int maxMethodComplexity,
            Map<Lambda, CompiledLambda> compiledLambdaMap,
            List<Parameter> contextArguments,
            Optional<ParentMethodContext> parentMethodContext)
    {
        this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
        this.mainReference = requireNonNull(mainReference, "mainReference is null");
        this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
        this.cachedInstanceBinder = requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        this.referenceCompiler = requireNonNull(referenceCompiler, "referenceCompiler is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.typeCoercion = new TypeCoercion(typeManager::getType);
        this.maxMethodComplexity = maxMethodComplexity;
        this.compiledLambdaMap = requireNonNull(compiledLambdaMap, "compiledLambdaMap is null");
        this.contextArguments = ImmutableList.copyOf(requireNonNull(contextArguments, "contextArguments is null"));
        this.parentMethodContext = requireNonNull(parentMethodContext, "parentMethodContext is null");
    }

    public BytecodeNode compile(Expression expression, Scope scope)
    {
        return compile(expression, scope, Optional.empty());
    }

    /**
     * Unlike {@link ExpressionBytecodeCompiler#compile(Expression, Scope)},
     * this method does not guarantee that the expression will be inlined into the provided scope.
     * If the scope is too complex, the expression may be extracted into a separate method,
     * and only a call to that method will be inlined in the scope.
     * <p>
     * This behavior may be undesirable in some cases — for example, when temporary variables
     * are used by child expressions. See {@link io.trino.sql.gen.BetweenCodeGenerator}. In such cases,
     * {@link ExpressionBytecodeCompiler#compile(Expression, Scope)} should be used instead.
     * <p>
     * Scope complexity is estimated based on the number of expressions already inlined that
     * the compiler is aware of.
     * <p>
     * Additionally, if the parent method context was not provided when instantiating the compiler,
     * extraction is effectively disabled.
     */
    public BytecodeNode compileWithExtraction(Expression expression, Scope scope)
    {
        if (parentMethodContext.isPresent() && getCurrentComplexity(scope) > maxMethodComplexity) {
            return extractToMethod(expression, scope, parentMethodContext.get());
        }
        return compile(expression, scope);
    }

    public BytecodeNode compile(Expression expression, Scope scope, Optional<Class<?>> lambdaInterface)
    {
        scopeComplexity.put(scope, getCurrentComplexity(scope) + 1);
        return new Visitor().process(expression, new Context(scope, lambdaInterface));
    }

    public BytecodeExpression mainReference()
    {
        return mainReference;
    }

    private int getCurrentComplexity(Scope scope)
    {
        return scopeComplexity.getOrDefault(scope, 0);
    }

    private BytecodeNode extractToMethod(Expression expression, Scope parentScope, ParentMethodContext parentMethodContext)
    {
        ClassScope evaluateClassScope = parentMethodContext.expressionContext().getCurrentChunkClass();
        io.trino.spi.type.Type type = expression.type();

        // Generate a method evaluating the expression
        MethodDefinition methodDefinition = evaluateClassScope.classDefinition().declareMethod(
                a(PUBLIC),
                "evaluateExpression_" + evaluateClassScope.getNextId(),
                type(type.getJavaType()),
                contextArguments);

        Scope methodScope = methodDefinition.getScope();
        BytecodeBlock methodBody = methodDefinition.getBody();

        // Declare context variables and fields
        Map<String, FieldDefinition> variableToFieldMap = new HashMap<>();
        for (Variable contextVariable : parentMethodContext.contextVariables()) {
            FieldDefinition field = evaluateClassScope.getOrCreateContextField(contextVariable.getType());
            methodScope.declareVariable(field.getType(), contextVariable.getName());
            variableToFieldMap.put(contextVariable.getName(), field);
        }
        restoreContextVariables(methodBody, methodScope, methodScope.getThis(), variableToFieldMap);
        BytecodeExpression evaluateMethodTarget = parentScope.getThis();
        ExpressionBytecodeCompiler compiler = this;
        if (!evaluateClassScope.classDefinition().equals(classDefinition)) {
            // the call will be to a method in a different chunk class
            BytecodeExpression chunkMainReference = methodScope.getThis().getField(
                    evaluateClassScope.classDefinition().getType(),
                    "__main",
                    parentMethodContext.expressionContext().mainClass().classDefinition().getType());
            // create ExpressionBytecodeCompiler with the chunk class
            compiler = new ExpressionBytecodeCompiler(
                    evaluateClassScope.classDefinition(),
                    chunkMainReference,
                    callSiteBinder,
                    evaluateClassScope.cachedInstanceBinder(),
                    referenceCompiler,
                    functionManager,
                    metadata,
                    typeManager,
                    maxMethodComplexity,
                    compiledLambdaMap,
                    contextArguments,
                    Optional.of(parentMethodContext));
            // update evaluateMethodTarget to point to a chunk object that will contain the newly generated method
            String chunkField = parentMethodContext.expressionContext().getChunkField(classDefinition, evaluateClassScope.classDefinition());
            evaluateMethodTarget = parentScope.getThis().getField(classDefinition.getType(), chunkField, evaluateClassScope.classDefinition().getType());
        }
        methodBody.append(compiler.compile(expression, methodScope));
        dumpContextVariables(methodBody, methodScope, methodScope.getThis(), variableToFieldMap);
        methodBody.ret(type.getJavaType());

        // Invoke the method
        BytecodeBlock invocation = new BytecodeBlock();
        dumpContextVariables(invocation, parentScope, evaluateMethodTarget, variableToFieldMap);
        invocation.append(evaluateMethodTarget.invoke(methodDefinition, contextArguments));
        restoreContextVariables(invocation, parentScope, evaluateMethodTarget, variableToFieldMap);

        evaluateClassScope.releaseAllContextFields();

        return invocation;
    }

    private static void dumpContextVariables(BytecodeBlock block, Scope scope, BytecodeExpression chunkReference, Map<String, FieldDefinition> variableToFieldMap)
    {
        for (Map.Entry<String, FieldDefinition> variableToField : variableToFieldMap.entrySet()) {
            Variable variable = scope.getVariable(variableToField.getKey());
            FieldDefinition field = variableToField.getValue();
            block.append(chunkReference.setField(field, variable));
        }
    }

    private static void restoreContextVariables(BytecodeBlock block, Scope scope, BytecodeExpression chunkReference, Map<String, FieldDefinition> variableToFieldMap)
    {
        for (Map.Entry<String, FieldDefinition> variableToField : variableToFieldMap.entrySet()) {
            Variable variable = scope.getVariable(variableToField.getKey());
            FieldDefinition field = variableToField.getValue();
            block.append(variable.set(chunkReference.getField(field)));
        }
    }

    private BytecodeGeneratorContext generatorContext(Scope scope)
    {
        return new BytecodeGeneratorContext(
                this,
                scope,
                callSiteBinder,
                cachedInstanceBinder,
                functionManager,
                metadata,
                classDefinition,
                contextArguments);
    }

    private static final String TEMP_PREFIX = "$$TEMP$$";

    public static Reference createTempReference(Variable variable, Type type)
    {
        return new Reference(type, TEMP_PREFIX + variable.getName());
    }

    @VisibleForTesting
    class Visitor
            extends IrVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode process(Expression node)
        {
            throw new UnsupportedOperationException("Process without context should not be called");
        }

        @Override
        protected BytecodeNode visitExpression(Expression node, Context context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression compiler for " + node.getClass().getName());
        }

        @Override
        protected BytecodeNode visitCall(Call node, Context context)
        {
            BytecodeGeneratorContext generatorContext = generatorContext(context.scope());
            return generatorContext.generateFullCall(node.function(), node.arguments());
        }

        @Override
        protected BytecodeNode visitConstant(Constant node, Context context)
        {
            Object value = node.value();
            Class<?> javaType = node.type().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                return block.comment("constant null")
                        .append(context.scope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }

            // use LDC for primitives (boolean, short, int, long, float, double)
            block.comment("constant " + node.type().getTypeSignature());
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
            Binding binding = callSiteBinder.bind(value, node.type().getJavaType());
            return new BytecodeBlock()
                    .setDescription("constant " + node.type())
                    .comment(node.toString())
                    .append(loadConstant(binding));
        }

        @Override
        protected BytecodeNode visitReference(Reference node, Context context)
        {
            if (node.name().startsWith(TEMP_PREFIX)) {
                return context.scope().getTempVariable(node.name().substring(TEMP_PREFIX.length()));
            }
            return referenceCompiler.apply(node, context.scope());
        }

        @Override
        protected BytecodeNode visitLambda(Lambda node, Context context)
        {
            checkState(compiledLambdaMap.containsKey(node), "lambda expressions map does not contain this lambda definition");
            if (!context.lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class)) {
                // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
                throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
            }

            BytecodeGeneratorContext generatorContext = generatorContext(context.scope());
            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(node),
                    context.lambdaInterface().get());
        }

        @Override
        protected BytecodeNode visitComparison(Comparison node, Context context)
        {
            BytecodeGeneratorContext generatorContext = generatorContext(context.scope());
            Expression left = node.left();
            Expression right = node.right();

            return switch (node.operator()) {
                case NOT_EQUAL -> {
                    ResolvedFunction equalsFunction = metadata.resolveOperator(
                            OperatorType.EQUAL,
                            ImmutableList.of(left.type(), right.type()));
                    ResolvedFunction notFunction = metadata.resolveBuiltinFunction("$not", fromTypes(BOOLEAN));
                    yield generatorContext.generateCall(notFunction,
                            ImmutableList.of(generatorContext.generateCall(equalsFunction,
                                    ImmutableList.of(generatorContext.generate(left), generatorContext.generate(right)))));
                }
                case GREATER_THAN -> generateComparisonCall(generatorContext, OperatorType.LESS_THAN, right, left);
                case GREATER_THAN_OR_EQUAL -> generateComparisonCall(generatorContext, OperatorType.LESS_THAN_OR_EQUAL, right, left);
                case EQUAL -> generateComparisonCall(generatorContext, OperatorType.EQUAL, left, right);
                case LESS_THAN -> generateComparisonCall(generatorContext, OperatorType.LESS_THAN, left, right);
                case LESS_THAN_OR_EQUAL -> generateComparisonCall(generatorContext, OperatorType.LESS_THAN_OR_EQUAL, left, right);
                case IDENTICAL -> generateComparisonCall(generatorContext, OperatorType.IDENTICAL, left, right);
            };
        }

        private BytecodeNode generateComparisonCall(BytecodeGeneratorContext generatorContext, OperatorType operatorType, Expression left, Expression right)
        {
            ResolvedFunction function = metadata.resolveOperator(operatorType, ImmutableList.of(left.type(), right.type()));
            return generatorContext.generateCall(function,
                    ImmutableList.of(generatorContext.generate(left), generatorContext.generate(right)));
        }

        @Override
        protected BytecodeNode visitCast(Cast node, Context context)
        {
            BytecodeGeneratorContext generatorContext = generatorContext(context.scope());
            Type returnType = node.type();
            Type sourceType = node.expression().type();

            // Type-only coercions (e.g., varchar(10) to varchar(20)) don't change the runtime
            // value — the Java type is the same. Simply compile the inner expression and use
            // the result directly, without generating a coercion function call.
            if (typeCoercion.isTypeOnlyCoercion(sourceType, returnType)) {
                return generatorContext.generate(node.expression());
            }

            ResolvedFunction coercion = metadata.getCoercion(sourceType, returnType);
            return generatorContext.generateFullCall(coercion, ImmutableList.of(node.expression()));
        }

        @Override
        protected BytecodeNode visitLogical(Logical node, Context context)
        {
            BytecodeGenerator generator = switch (node.operator()) {
                case AND -> new AndCodeGenerator(node);
                case OR -> new OrCodeGenerator(node);
            };
            return generator.generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitCase(Case node, Context context)
        {
            // Generate nested IF bytecode: IF(cond1, val1, IF(cond2, val2, ... default))
            BytecodeGeneratorContext generatorContext = generatorContext(context.scope());
            BytecodeNode result = generatorContext.generateWithExtraction(node.defaultValue());

            for (WhenClause clause : node.whenClauses().reversed()) {
                Variable wasNull = generatorContext.wasNull();
                BytecodeBlock conditionBlock = new BytecodeBlock()
                        .append(generatorContext.generateWithExtraction(clause.getOperand()))
                        .comment("... and condition value was not null")
                        .append(wasNull)
                        .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                        .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                        .append(wasNull.set(constantFalse()));

                result = new IfStatement()
                        .condition(conditionBlock)
                        .ifTrue(generatorContext.generateWithExtraction(clause.getResult()))
                        .ifFalse(result);
            }
            return result;
        }

        @Override
        protected BytecodeNode visitSwitch(Switch node, Context context)
        {
            return new SwitchCodeGenerator(node, metadata).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitCoalesce(Coalesce node, Context context)
        {
            return new CoalesceCodeGenerator(node).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitIsNull(IsNull node, Context context)
        {
            return new IsNullCodeGenerator(node).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitNullIf(NullIf node, Context context)
        {
            return new NullIfCodeGenerator(node, metadata).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitBetween(Between node, Context context)
        {
            return new BetweenCodeGenerator(node, metadata).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitIn(In node, Context context)
        {
            return new InCodeGenerator(node, metadata).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitFieldReference(FieldReference node, Context context)
        {
            return new DereferenceCodeGenerator(node).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitRow(Row node, Context context)
        {
            return new RowConstructorCodeGenerator(node).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitArray(Array node, Context context)
        {
            return new ArrayConstructorCodeGenerator(node).generateExpression(generatorContext(context.scope()));
        }

        @Override
        protected BytecodeNode visitBind(Bind node, Context context)
        {
            return new BindCodeGenerator(node, compiledLambdaMap, context.lambdaInterface().get())
                    .generateExpression(generatorContext(context.scope()));
        }
    }

    @VisibleForTesting
    record Context(Scope scope, Optional<Class<?>> lambdaInterface) {}
}
