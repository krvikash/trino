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

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

public class VendorTestModels
{
    public static final String LANGUAGE_MODEL_ID = "language_model";
    public static final String EMBED_MODEL_ID = "embed_model";
    private static final String AZURE_OPEN_AI_LANGUAGE_ENDPOINT = requireEnv("AZURE_OPEN_AI_LANGUAGE_ENDPOINT");
    private static final String AZURE_OPEN_AI_EMBED_ENDPOINT = requireEnv("AZURE_OPEN_AI_EMBED_ENDPOINT");

    public static final String AZURE_OPEN_AI_MODEL_PROVIDERS = """
                {
                  "models": [
                     {
                        "id": "%s",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "%s",
                            "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                        }
                    },
                    {
                      "id": "%s",
                      "modelName": "text-embedding-3-small",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "%s",
                        "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                      }
                    }
                  ]
                }""".formatted(LANGUAGE_MODEL_ID, AZURE_OPEN_AI_LANGUAGE_ENDPOINT, EMBED_MODEL_ID, AZURE_OPEN_AI_EMBED_ENDPOINT);

    public static final String GEMINI_MODEL_PROVIDERS = """
                {
                  "models": [
                     {
                        "id": "%s",
                        "modelName": "gemini-2.0-flash",
                        "kind": "GENERATE",
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://generativelanguage.googleapis.com/v1beta/openai/",
                            "apiKey": "${ENV:GEMINI_API_KEY}"
                        }
                    },
                    {
                      "id": "%s",
                      "modelName": "gemini-embedding-001",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "https://generativelanguage.googleapis.com/v1beta/openai/",
                        "apiKey": "${ENV:GEMINI_API_KEY}"
                      }
                    }
                  ]
                }""".formatted(LANGUAGE_MODEL_ID, EMBED_MODEL_ID);

    private VendorTestModels() {}
}
