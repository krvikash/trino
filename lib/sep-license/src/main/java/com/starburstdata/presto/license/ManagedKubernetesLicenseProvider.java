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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Cloud Marketplaces offers container products that need to be integrated with respective license and/or billing services in a unique manner.
 * This class initialize SEP license if provided with specific data from a sidecar container deployed together with SEP to the same Kubernetes Pod.
 * In this architecture sidecar is responsible for all communications related to license and billing handling.
 * <p>
 * 'ManagedKubernetesLicenseProvider' name derives from the fact that Cloud Providers intend to deploy containers bought in their Marketplaces to their respective Kubernetes services.
 * 'JSONLicenseProvider' is a better way to go if deploying on-prem or after purchase being done directly at Starburst.
 *
 * @see <a href="https://starburstdata.atlassian.net/wiki/spaces/STARBURST/pages/2303557633/AWS+Marketplace+Containers+-+project+Totem">Market place containers Confluence page</a>
 * @see <a href="https://github.com/starburstdata/starburst-marketplace-containers">Kubernetes container sidecar implementation</a>
 */
class ManagedKubernetesLicenseProvider
        implements LicenseProvider
{
    private enum Cloud
    {
        AWS("aws", LicenseType.AWS_EKS),
        GCP("gcp", LicenseType.GCP_GKE),
        AZURE("azure", LicenseType.AZURE_AKS),
        REDHAT("redhat", LicenseType.RH_OPENSHIFT),
        OTHER("other", LicenseType.UNKNOWN);

        private final String name;
        private final LicenseType licenseType;

        Cloud(String name, LicenseType licenseType)
        {
            this.name = name;
            this.licenseType = licenseType;
        }

        public LicenseType getLicenseType()
        {
            return this.licenseType;
        }

        public static Cloud fromString(String txt)
        {
            for (Cloud cloud : Cloud.values()) {
                if (cloud.name.equalsIgnoreCase(txt)) {
                    return cloud;
                }
            }
            return null;
        }
    }

    private static final Logger log = Logger.get(ManagedKubernetesLicenseProvider.class);
    private static final JsonCodec<ManagedKubernetesIdentityDocument> MANAGED_KUBERNETES_IDENTITY_JSON_CODEC = new JsonCodecFactory().jsonCodec(ManagedKubernetesIdentityDocument.class);
    private static final String PROVIDER_NAME = "Starburst Enterprise Managed Kubernetes";
    private static final String PROVIDER_ENV_VARIABLE = "ENABLE_K8S_LICENSE_PROVIDER";
    private static final String MAX_RETRY_ENV_VARIABLE = "K8S_LICENSE_PROVIDER_MAX_RETRY";
    private static final String PORT_ENV_VARIABLE = "K8S_LICENSE_PROVIDER_PORT";
    private static final String MANAGED_KUBERNETES_LICENSE_ENDPOINT = String.format("http://127.0.0.1:%s/license", System.getenv().getOrDefault(PORT_ENV_VARIABLE, "8085"));
    private static final String LICENSE_INIT_SUCCESS_STATUS = "success";
    private static final String LICENSE_INIT_FAILED_STATUS = "failure";
    private static final String LICENSE_INIT_ERROR_STATUS = "crash";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static License managedKubernetesLicense;

    private final URLRequester urlRequester;

    public ManagedKubernetesLicenseProvider()
    {
        this(new URLRequesterImpl());
    }

    @VisibleForTesting
    ManagedKubernetesLicenseProvider(URLRequester urlRequester)
    {
        this.urlRequester = urlRequester;
    }

    static void setManagedKubernetesLicense(License license)
    {
        managedKubernetesLicense = license;
    }

    @Override
    public Optional<License> getLicense()
    {
        String identity;
        Cloud cloudUsed;
        byte[] identityJson = null;

        // Return empty if ManagedKubernetesLicenseProvider is disabled
        if (!Boolean.parseBoolean(System.getenv().getOrDefault(PROVIDER_ENV_VARIABLE, "false"))) {
            log.info("%s is disabled", PROVIDER_NAME);
            return Optional.empty();
        }

        // Sep seems to check license for each plugin
        if (managedKubernetesLicense != null) {
            return Optional.of(managedKubernetesLicense);
        }

        int retry = 0;
        int connectMaxRetry = Integer.parseInt(System.getenv().getOrDefault(MAX_RETRY_ENV_VARIABLE, "120"));
        while (retry < connectMaxRetry) {
            try {
                if (retry != 0) {
                    Thread.sleep(1000);
                }
                identityJson = urlRequester.get(URI.create(MANAGED_KUBERNETES_LICENSE_ENDPOINT).toURL(), CONNECT_TIMEOUT);
                break;
            }
            catch (IOException | InterruptedException e) {
                log.warn("%s License not available, retrying...", PROVIDER_NAME);
                retry++;
            }
        }
        if (identityJson == null) {
            log.info("Failed to initialize %s License. Possible networking issues within Kubernetes Pod", PROVIDER_NAME);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("%s License Verifier failed to start", PROVIDER_NAME));
        }

        try {
            ManagedKubernetesIdentityDocument identityDocument = MANAGED_KUBERNETES_IDENTITY_JSON_CODEC.fromJson(identityJson);
            switch (identityDocument.getStatus().toLowerCase(Locale.ENGLISH)) {
                case LICENSE_INIT_SUCCESS_STATUS:
                    identity = identityDocument.getAccountId();
                    cloudUsed = Cloud.fromString(identityDocument.getCloud());
                    break;
                case LICENSE_INIT_FAILED_STATUS:
                    log.info("Failed to checkout %s License. Verify entitlements under Marketplace license grant", PROVIDER_NAME);
                    return Optional.empty();
                case LICENSE_INIT_ERROR_STATUS:
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Internal %s License Verifier error", PROVIDER_NAME));
                default:
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Response from %s License Verifier not supported", PROVIDER_NAME));
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Problem encountered while checking the %s identity document", PROVIDER_NAME), e);
        }
        if (cloudUsed == null) {
            log.info("Cloud provider not supported by %s License", PROVIDER_NAME);
            return Optional.empty();
        }
        log.info("%s License successfully initialized", PROVIDER_NAME);
        setManagedKubernetesLicense(License.unsigned(identity, cloudUsed.getLicenseType(), LocalDateTime.MAX));
        return Optional.of(managedKubernetesLicense);
    }
}
