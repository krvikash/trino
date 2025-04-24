/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.google.common.base.Throwables.getCausalChain;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCreateModelConnectionSpecs
{
    @Test
    public void testShouldMaskSecretsWhenParsingFails()
    {
        String json = """
              {
                 "models": [
                     {
                         "id": "embed1",
                         "modelName": "cohere.embed-multilingual-v3",
                         "kind": "EMBED",
                         "dimensions": 768,
                         "connectionInfo": {
                             "provider": "AWS_BEDROCK",
                             "awsAccessKey": MYSECRET
                             "region": "us-east-1"
                         }
                     }
                 ]
              }""";

        File file = createModelConnectionSpecsFile(json);

        FileBackedModelConnectionSpecsLoader reader = new FileBackedModelConnectionSpecsLoader(
                new AiFileStorageConfig().setModelConnectionSpecsFile(file.getPath()),
                new SecretsResolver(ImmutableMap.of()));
        assertThatThrownBy(reader::load)
                .satisfies(exception -> assertThat(getCausalChain(exception)
                        .stream()
                        .map(Throwable::getMessage))
                        .noneMatch(message -> message.contains("MYSECRET")));
    }
}
