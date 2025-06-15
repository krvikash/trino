/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

class AWSMarketplaceLicenseProvider
        implements LicenseProvider
{
    private static final Logger log = Logger.get(AWSMarketplaceLicenseProvider.class);
    private static final JsonCodec<AWSIdentityDocument> AWS_IDENTITY_DOCUMENT_JSON_CODEC = new JsonCodecFactory().jsonCodec(AWSIdentityDocument.class);
    private static final Set<String> STARBURST_PRESTO_MARKETPLACE_PRODUCT_CODES =
            ImmutableSet.of("yh32yzm07ctyg8b4gpsk127n", "6asvaypvo3rvqrp89gypcdsfj", "dybtclj3gmg31yn6v0fewejlp", "crbsyywsx0717p9tphd3qjl3r");

    private static final String METADATA_SERVICE_BASE_URL = "http://169.254.169.254";
    private static final String METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH = "/latest/dynamic/instance-identity/";
    private static final String INSTANCE_IDENTITY_ROOT = METADATA_SERVICE_BASE_URL + METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH;

    private final InstanceIdentityVerifier identityVerifier;
    private final Set<String> marketplaceProductCodes;
    private final URLRequester urlRequester;

    public AWSMarketplaceLicenseProvider()
    {
        this(new AWSInstanceIdentityVerifier(), STARBURST_PRESTO_MARKETPLACE_PRODUCT_CODES, new URLRequesterImpl());
    }

    @VisibleForTesting
    AWSMarketplaceLicenseProvider(InstanceIdentityVerifier identityVerifier, Set<String> marketplaceProductCodes, URLRequester urlRequester)
    {
        this.identityVerifier = identityVerifier;
        this.marketplaceProductCodes = marketplaceProductCodes == null ? ImmutableSet.of() : ImmutableSet.copyOf(marketplaceProductCodes);
        this.urlRequester = urlRequester;
    }

    @Override
    public Optional<License> getLicense()
    {
        String token;

        try {
            // Ability to connect to metadata service token endpoint confirms that we are running in AWS ecosystem
            token = new String(urlRequester.put(
                    URI.create(METADATA_SERVICE_BASE_URL + "/latest/api/token").toURL(),
                    Duration.ofSeconds(10),
                    ImmutableMap.of("X-aws-ec2-metadata-token-ttl-seconds", "180")),
                    StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            // Apparently running outside of AWS (or the network is misbehaving)
            log.debug(e, "AWS instance metadata service endpoint not reachable: %s", e);
            return Optional.empty();
        }

        try {
            byte[] identityJson = urlRequester.get(
                    URI.create(INSTANCE_IDENTITY_ROOT + "document").toURL(),
                    Duration.ofSeconds(10),
                    ImmutableMap.of("X-aws-ec2-metadata-token", token));
            byte[] base64Signature = urlRequester.get(
                    URI.create(INSTANCE_IDENTITY_ROOT + "signature").toURL(),
                    Duration.ofSeconds(10),
                    ImmutableMap.of("X-aws-ec2-metadata-token", token));
            identityVerifier.verify(identityJson, base64Signature);
            AWSIdentityDocument identityDocument = AWS_IDENTITY_DOCUMENT_JSON_CODEC.fromJson(identityJson);
            if (!Sets.intersection(identityDocument.getMarketplaceProductCodes(), marketplaceProductCodes).isEmpty()) {
                return Optional.of(License.unsignedAllFeatures(identityDocument.getAccountId(), LicenseType.AWS, LocalDateTime.MAX));
            }
            log.info("Did not find Starburst Enterprise AWS Marketplace license");
            return Optional.empty();
        }
        catch (IllegalArgumentException | IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Problem encountered while checking the AWS instance identity document", e);
        }
    }
}
