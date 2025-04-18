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
package io.trino.plugin.opensearch.client;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.BinaryUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.trino.plugin.opensearch.AwsSecurityConfig.DeploymentType;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;

class AwsRequestSigner
        implements HttpRequestInterceptor
{
    private static final String EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private final String serviceName;
    private final AWSCredentialsProvider credentialsProvider;
    private final AWS4Signer signer;

    public AwsRequestSigner(String region, DeploymentType deploymentType, AWSCredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = credentialsProvider;
        this.signer = new AWS4Signer();
        this.serviceName = switch (deploymentType) {
            case SERVERLESS -> "aoss";
            case PROVISIONED -> "es";
        };

        signer.setServiceName(serviceName);
        signer.setRegionName(region);
    }

    @Override
    public void process(HttpRequest request, HttpContext context)
            throws IOException
    {
        String method = request.getRequestLine().getMethod();

        URI uri = URI.create(request.getRequestLine().getUri());
        URIBuilder uriBuilder = new URIBuilder(uri);

        Map<String, List<String>> parameters = new TreeMap<>(CASE_INSENSITIVE_ORDER);
        for (NameValuePair parameter : uriBuilder.getQueryParams()) {
            parameters.computeIfAbsent(parameter.getName(), key -> new ArrayList<>())
                    .add(parameter.getValue());
        }
        // reuse contentBytes for both signing and post-signing. InputStream will be consumed post signing, setting the request entity content to empty
        byte[] contentBytes = null;
        if (request instanceof HttpEntityEnclosingRequest enclosingRequest && enclosingRequest.getEntity() != null) {
            InputStream contentStream = enclosingRequest.getEntity().getContent();
            contentBytes = contentStream.readAllBytes();
        }

        // Serverless requires payload hash
        String payloadHash;
        try {
            MessageDigest sha256Digest = MessageDigest.getInstance("SHA-256");

            if (contentBytes != null) {
                payloadHash = BinaryUtils.toHex(sha256Digest.digest(contentBytes));
            }
            else {
                payloadHash = EMPTY_BODY_SHA256;
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.builder();
        Arrays.stream(request.getAllHeaders())
                .forEach(header -> headersBuilder.put(header.getName(), header.getValue()));
        headersBuilder.put("X-Amz-Content-Sha256", payloadHash);

        DefaultRequest<?> awsRequest = new DefaultRequest<>(serviceName);

        HttpHost host = (HttpHost) context.getAttribute(HTTP_TARGET_HOST);
        if (host != null) {
            awsRequest.setEndpoint(URI.create(host.toURI()));
        }
        awsRequest.setHttpMethod(HttpMethodName.fromValue(method));
        awsRequest.setResourcePath(uri.getRawPath());
        if (contentBytes != null) {
            awsRequest.setContent(new ByteArrayInputStream(contentBytes));
        }
        awsRequest.setParameters(parameters);
        awsRequest.setHeaders(headersBuilder.buildOrThrow());

        signer.sign(awsRequest, credentialsProvider.getCredentials());

        Header[] newHeaders = awsRequest.getHeaders().entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);

        request.setHeaders(newHeaders);

        if (contentBytes != null) {
            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(new ByteArrayInputStream(contentBytes));
            entity.setContentLength(contentBytes.length);
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }
    }
}
