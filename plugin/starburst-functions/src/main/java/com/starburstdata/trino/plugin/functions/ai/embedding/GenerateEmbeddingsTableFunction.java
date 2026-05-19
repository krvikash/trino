/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai.embedding;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.FunctionsMetadata;
import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingType;
import io.starburst.ai.client.ModelClientProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.DescriptorArgumentSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.DescriptorArgument.NULL_DESCRIPTOR;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class GenerateEmbeddingsTableFunction
        extends AbstractConnectorTableFunction
{
    private static final String SOURCE_ARGUMENT_NAME = "SOURCE";
    private static final String DATA_COLUMN_ARGUMENT_NAME = "DATA_COLUMN";
    private static final String EMBEDDING_COLUMN_ARGUMENT_NAME = "EMBEDDING_COLUMN";
    private static final String MODEL_ID_ARGUMENT_NAME = "MODEL_ID";
    private static final Map<TypeSignature, EmbeddingType> SUPPORTED_TYPES = ImmutableMap.<TypeSignature, EmbeddingType>builder()
            .put(new ArrayType(RealType.REAL).getTypeSignature(), EmbeddingType.FLOAT)
            .put(new ArrayType(DoubleType.DOUBLE).getTypeSignature(), EmbeddingType.FLOAT)
            .put(VarbinaryType.VARBINARY.getTypeSignature(), EmbeddingType.BINARY)
            .buildOrThrow();

    private final AiModelAccessControl aiModelAccessControl;

    @Inject
    public GenerateEmbeddingsTableFunction(AiModelAccessControl aiModelAccessControl)
    {
        super(FunctionsMetadata.AI_SCHEMA_NAME,
                "generate_embeddings",
                ImmutableList.of(
                        TableArgumentSpecification.builder()
                                .name(SOURCE_ARGUMENT_NAME)
                                .passThroughColumns()
                                .rowSemantics()
                                .build(),
                        DescriptorArgumentSpecification.builder()
                                .name(DATA_COLUMN_ARGUMENT_NAME)
                                .build(),
                        DescriptorArgumentSpecification.builder()
                                .name(EMBEDDING_COLUMN_ARGUMENT_NAME)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(MODEL_ID_ARGUMENT_NAME)
                                .type(VarcharType.VARCHAR)
                                .build()),
                GENERIC_TABLE);
        this.aiModelAccessControl = requireNonNull(aiModelAccessControl, "aiModelAccessControl is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        DescriptorArgument contentColumn = (DescriptorArgument) arguments.get(DATA_COLUMN_ARGUMENT_NAME);
        if (contentColumn.equals(NULL_DESCRIPTOR)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "DATA_COLUMN descriptor is null");
        }
        Descriptor contentColumnDescriptor = contentColumn.getDescriptor().orElseThrow();
        if (contentColumnDescriptor.getFields().stream().anyMatch(field -> field.getType().isPresent())) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "DATA_COLUMN descriptor contains types");
        }

        if (contentColumnDescriptor.getFields().size() != 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "DATA_COLUMN descriptor contains more than one column");
        }

        DescriptorArgument embeddingColumn = (DescriptorArgument) arguments.get(EMBEDDING_COLUMN_ARGUMENT_NAME);
        if (contentColumn.equals(NULL_DESCRIPTOR)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "EMBEDDING_COLUMN descriptor is null");
        }
        Descriptor embeddingColumnDescriptor = embeddingColumn.getDescriptor().orElseThrow();
        if (embeddingColumnDescriptor.getFields().size() != 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "EMBEDDING_COLUMN descriptor contains more than one column");
        }
        Descriptor.Field embeddingColumnField = getOnlyElement(embeddingColumnDescriptor.getFields());
        Type embeddingType = new ArrayType(RealType.REAL);
        if (embeddingColumnField.getType().isPresent()) {
            TypeSignature specifiedTypeSignature = embeddingColumnField.getType().get().getTypeSignature();
            if (!SUPPORTED_TYPES.containsKey(specifiedTypeSignature)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "EMBEDDING_COLUMN descriptor references an unsupported type: " + specifiedTypeSignature);
            }

            embeddingType = embeddingColumnField.getType().get();
        }

        ScalarArgument modelIdArgument = (ScalarArgument) arguments.get(MODEL_ID_ARGUMENT_NAME);
        checkFunctionArgument(modelIdArgument.getValue() != null, "MODEL_ID value cannot be null");
        Slice modelId = ((Slice) modelIdArgument.getValue());
        checkFunctionArgument(modelId.length() > 0, "MODEL_ID value cannot be empty");
        aiModelAccessControl.checkCanExecuteModel(new AiModelAccessControl.Context(session), modelId.toStringUtf8());

        String dataColumnName = getOnlyElement(contentColumnDescriptor.getFields()).getName().orElseThrow().toLowerCase(ENGLISH);

        List<RowType.Field> inputSchema = ((TableArgument) arguments.get(SOURCE_ARGUMENT_NAME)).getRowType().getFields();
        Set<String> inputNames = inputSchema.stream()
                .map(RowType.Field::getName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableSet());

        if (inputNames.contains(getOnlyElement(embeddingColumnDescriptor.getFields()).getName().orElseThrow().toLowerCase(ENGLISH))) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Embedding column must not be present in SOURCE input");
        }
        if (!inputNames.contains(dataColumnName)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Column %s not present in the table", dataColumnName));
        }

        ImmutableList.Builder<Integer> requiredColumns = ImmutableList.builder();
        for (int i = 0; i < inputSchema.size(); i++) {
            Optional<String> columnName = inputSchema.get(i).getName();
            if (columnName.map(name -> name.equalsIgnoreCase(dataColumnName)).orElse(false)) {
                requiredColumns.add(i);
            }
        }

        ImmutableList.Builder<Descriptor.Field> returnedColumns = ImmutableList.builder();
        returnedColumns.add(new Descriptor.Field(getOnlyElement(embeddingColumnDescriptor.getFields()).getName().orElseThrow(), Optional.of(embeddingType)));

        return TableFunctionAnalysis.builder()
                .requiredColumns(SOURCE_ARGUMENT_NAME, requiredColumns.build())
                .returnedType(new Descriptor(returnedColumns.build()))
                .handle(new GenerateEmbeddingsFunctionHandle(modelId, SUPPORTED_TYPES.get(embeddingType.getTypeSignature())))
                .build();
    }

    public static TableFunctionProcessorProvider getGenerateEmbeddingsFunctionProcessorProvider(ModelClientProvider clientProvider)
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
            {
                GenerateEmbeddingsFunctionHandle functionHandle = (GenerateEmbeddingsFunctionHandle) handle;
                return new GenerateEmbeddingsFunctionDataProcessor(clientProvider.embeddingModelClient(functionHandle.modelId()), functionHandle.embeddingType());
            }
        };
    }
}
