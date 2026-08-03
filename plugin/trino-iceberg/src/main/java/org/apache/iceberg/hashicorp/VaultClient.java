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
package org.apache.iceberg.hashicorp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * HTTP client for interacting with HashiCorp Vault REST API.
 *
 * @see <a href=https://developer.hashicorp.com/vault/api-docs>HashiCorp Vault HTTP API</a>
 */
class VaultClient
        implements Closeable
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VAULT_TOKEN_HEADER = "X-Vault-Token";

    private final String address;

    private transient volatile CloseableHttpClient httpClient;

    VaultClient(String address)
    {
        this.address = requireNonNull(address, "address is null");
    }

    String encrypt(String vaultToken, String wrappingKeyId, String plaintext)
    {
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("plaintext", plaintext);

        JsonNode response = post("/v1/transit/encrypt/" + wrappingKeyId, vaultToken, requestBody);

        JsonNode dataNode = response.get("data");
        if (dataNode == null || !dataNode.has("ciphertext")) {
            throw new RuntimeException("Failed to wrap key: no ciphertext returned");
        }

        return dataNode.get("ciphertext").asText();
    }

    String decrypt(String vaultToken, String wrappingKeyId, String ciphertext)
    {
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("ciphertext", ciphertext);

        JsonNode response =
                post("/v1/transit/decrypt/" + wrappingKeyId, vaultToken, requestBody);

        JsonNode dataNode = response.get("data");
        if (dataNode == null || !dataNode.has("plaintext")) {
            throw new RuntimeException("Failed to unwrap key: no plaintext returned");
        }

        return dataNode.get("plaintext").asText();
    }

    DataKey generateKey(String vaultToken, String wrappingKeyId)
    {
        ObjectNode requestBody = MAPPER.createObjectNode();

        JsonNode response = post("/v1/transit/datakey/plaintext/" + wrappingKeyId, vaultToken, requestBody);

        JsonNode dataNode = response.get("data");
        if (dataNode == null || !dataNode.has("plaintext") || !dataNode.has("ciphertext")) {
            throw new RuntimeException("Failed to generate key: missing plaintext or ciphertext");
        }

        String plaintext = dataNode.get("plaintext").asText();
        String ciphertext = dataNode.get("ciphertext").asText();
        return new DataKey(plaintext, ciphertext);
    }

    private JsonNode post(String path, String token, ObjectNode requestBody)
    {
        HttpPost request = new HttpPost(address + path);
        if (token != null) {
            request.setHeader(VAULT_TOKEN_HEADER, token);
        }

        try {
            request.setEntity(
                    new StringEntity(MAPPER.writeValueAsString(requestBody), ContentType.APPLICATION_JSON));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize request body", e);
        }

        try {
            return httpClient().execute(request, HttpClientContext.create(), new VaultResponseHandler());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to execute Vault request to " + path, e);
        }
    }

    private static class VaultResponseHandler
            implements HttpClientResponseHandler<JsonNode>
    {
        @Override
        public JsonNode handleResponse(ClassicHttpResponse response)
        {
            int statusCode = response.getCode();
            try {
                String responseBody = response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity());
                checkState(statusCode == 200, "Vault request failed with status %s: %s", statusCode, responseBody);
                return MAPPER.readTree(responseBody);
            }
            catch (ParseException e) {
                throw new RuntimeException("Failed to parse Vault error response", e);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to read response", e);
            }
        }
    }

    private CloseableHttpClient httpClient()
    {
        if (httpClient == null) {
            synchronized (this) {
                if (httpClient == null) {
                    httpClient = HttpClients.createDefault();
                }
            }
        }
        return httpClient;
    }

    @Override
    public void close()
    {
        synchronized (this) {
            CloseableHttpClient client = httpClient;
            httpClient = null;
            if (client != null) {
                try {
                    client.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to close HTTP client", e);
                }
            }
        }
    }

    record DataKey(String plaintext, String ciphertext) {}
}
