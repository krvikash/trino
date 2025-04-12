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
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.starburst.ai.client.LanguageModelConnectionSpec;
import io.starburst.ai.client.ModelConnectionSpecDao;
import io.starburst.ai.client.PromptOverrides;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.starburstdata.trino.plugin.ai.AiMetadata.SCHEMA_NAME;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class LanguageModelSystemTable
        extends AiSystemTable<LanguageModelConnectionSpec>
{
    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName(SCHEMA_NAME, "language_models"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("id", VARCHAR))
                    .add(new ColumnMetadata("provider", VARCHAR))
                    .add(new ColumnMetadata("name", VARCHAR))
                    .add(new ColumnMetadata("endpoint", VARCHAR))
                    .add(new ColumnMetadata("max_tokens", INTEGER))
                    .add(new ColumnMetadata("temperature", REAL))
                    .add(new ColumnMetadata("top_p", REAL))
                    .add(new ColumnMetadata("system_prompts", new ArrayType(VARCHAR)))
                    .add(new ColumnMetadata("analyze_sentiment_prompt", VARCHAR))
                    .add(new ColumnMetadata("analyze_sentiment_system_prompt", VARCHAR))
                    .add(new ColumnMetadata("classify_prompt", VARCHAR))
                    .add(new ColumnMetadata("classify_system_prompt", VARCHAR))
                    .add(new ColumnMetadata("fix_grammar_prompt", VARCHAR))
                    .add(new ColumnMetadata("fix_grammar_system_prompt", VARCHAR))
                    .add(new ColumnMetadata("mask_prompt", VARCHAR))
                    .add(new ColumnMetadata("mask_system_prompt", VARCHAR))
                    .add(new ColumnMetadata("translate_prompt", VARCHAR))
                    .add(new ColumnMetadata("translate_system_prompt", VARCHAR))
                    .build());

    private final ModelConnectionSpecDao modelConnectionSpecDao;

    @Inject
    public LanguageModelSystemTable(ModelConnectionSpecDao modelConnectionSpecDao)
    {
        super(METADATA);
        this.modelConnectionSpecDao = requireNonNull(modelConnectionSpecDao, "modelConnectionSpecProvider is null");
    }

    @Override
    protected List<?> toRow(LanguageModelConnectionSpec spec)
    {
        List<Object> row = new ArrayList<>();
        row.add(spec.id());
        row.add(convertProvider(spec.connectionInfo()));
        row.add(spec.modelName());
        row.add(getEndpoint(spec));
        row.add(spec.maxTokens().orElse(null));
        row.add(convertFloat(spec.temperature()));
        row.add(convertFloat(spec.topP()));
        row.add(spec.prompts().flatMap(PromptOverrides::systemPrompts).map(LanguageModelSystemTable::convertList).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::analyzeSentimentPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::analyzeSentimentSystemPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::classifyPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::classifySystemPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::fixGrammarPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::fixGrammarSystemPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::maskPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::maskSystemPrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::translatePrompt).orElse(null));
        row.add(spec.prompts().flatMap(PromptOverrides::translateSystemPrompt).orElse(null));
        return row;
    }

    @Override
    protected Collection<LanguageModelConnectionSpec> getSpecs()
    {
        return modelConnectionSpecDao.languageModelConnectionSpecs();
    }
}
