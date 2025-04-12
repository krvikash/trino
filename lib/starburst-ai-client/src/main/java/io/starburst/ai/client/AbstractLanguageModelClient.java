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
package io.starburst.ai.client;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;

public abstract class AbstractLanguageModelClient
        implements LanguageModelClient
{
    protected static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);
    protected static final JsonCodec<String> STRING_CODEC = jsonCodec(String.class);

    protected final PromptDao promptDao;

    protected final List<String> topLevelSystemPrompts;

    protected final ModelWithFixedPrompt analyzeSentimentModelAndPrompt;
    protected final ModelWithFixedPrompt classifyModelAndPrompt;
    protected final ModelWithFixedPrompt fixGrammarModelAndPrompt;
    protected final ModelWithFixedPrompt maskModelAndPrompt;
    protected final ModelWithFixedPrompt translateModelAndPrompt;
    protected final String generateModel;

    protected AbstractLanguageModelClient(String model, PromptDao promptDao)
    {
        this.generateModel = requireNonNull(model, "model is null");
        this.promptDao = requireNonNull(promptDao, "promptDao is null");
        this.topLevelSystemPrompts = promptDao.systemPrompts();
        this.analyzeSentimentModelAndPrompt = create("analyzeSentiment", generateModel, topLevelSystemPrompts, promptDao.analyzeSentimentPrompt(), promptDao.analyzeSentimentSystemPrompt());
        this.classifyModelAndPrompt = create("classify", generateModel, topLevelSystemPrompts, promptDao.classifyPrompt(), promptDao.classifySystemPrompt());
        this.fixGrammarModelAndPrompt = create("fixGrammar", generateModel, topLevelSystemPrompts, promptDao.fixGrammarPrompt(), promptDao.fixGrammarSystemPrompt());
        this.maskModelAndPrompt = create("mask", generateModel, topLevelSystemPrompts, promptDao.maskPrompt(), promptDao.maskSystemPrompt());
        this.translateModelAndPrompt = create("translate", generateModel, topLevelSystemPrompts, promptDao.translatePrompt(), promptDao.translateSystemPrompt());
    }

    @Override
    public String analyzeSentiment(String text)
    {
        return fixedCompletion(analyzeSentimentModelAndPrompt, analyzeSentimentModelAndPrompt.prompt().formatted(text));
    }

    @Override
    public String classify(String text, List<String> labels)
    {
        return fixedCompletion(classifyModelAndPrompt, formatLabelsAndText(classifyModelAndPrompt.prompt(), labels, text));
    }

    @Override
    public String fixGrammar(String text)
    {
        return fixedCompletion(fixGrammarModelAndPrompt, fixGrammarModelAndPrompt.prompt().formatted(text));
    }

    @Override
    public String generate(String prompt)
    {
        return completion(prompt, Optional.empty());
    }

    @Override
    public String generate(String systemPrompt, String prompt)
    {
        return completion(prompt, Optional.of(systemPrompt));
    }

    @Override
    public String mask(String text, List<String> labels)
    {
        return fixedCompletion(maskModelAndPrompt, formatLabelsAndText(maskModelAndPrompt.prompt(), labels, text));
    }

    @Override
    public String translate(String text, String language)
    {
        return fixedCompletion(translateModelAndPrompt, translateModelAndPrompt.prompt().formatted(STRING_CODEC.toJson(language), text));
    }

    private String completion(String prompt, Optional<String> system)
    {
        List<String> systemPrompts;
        if (system.isPresent()) {
            systemPrompts = ImmutableList.<String>builder()
                    .addAll(topLevelSystemPrompts)
                    .add(system.get())
                    .build();
        }
        else {
            systemPrompts = topLevelSystemPrompts;
        }
        return generateCompletion(this.generateModel, systemPrompts, prompt);
    }

    private String fixedCompletion(ModelWithFixedPrompt modelAndPrompt, String userPrompt)
    {
        return completion(modelAndPrompt.model(), modelAndPrompt.systemPrompts(), userPrompt);
    }

    private String completion(String model, List<String> systemPrompts, String userPrompt)
    {
        return generateCompletion(model, systemPrompts, userPrompt);
    }

    private static String formatLabelsAndText(String template, List<String> labels, String text)
    {
        return template.formatted(LIST_CODEC.toJson(labels), text);
    }

    protected abstract String generateCompletion(String model, List<String> systemPrompts, String prompt);

    protected record ModelWithFixedPrompt(String name, String model, List<String> systemPrompts, String prompt) {}

    protected static ModelWithFixedPrompt create(String name, String model, List<String> topLevelSystemPrompts, String userPrompt, Optional<String> systemPrompt)
    {
        List<String> systemPrompts;
        if (systemPrompt.isPresent()) {
            systemPrompts = ImmutableList.<String>builder()
                    .addAll(topLevelSystemPrompts)
                    .add(systemPrompt.get())
                    .build();
        }
        else {
            systemPrompts = topLevelSystemPrompts;
        }
        return new ModelWithFixedPrompt(name, model, systemPrompts, userPrompt);
    }
}
