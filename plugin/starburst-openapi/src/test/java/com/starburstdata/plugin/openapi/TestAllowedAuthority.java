/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAllowedAuthority
{
    private static final AllowedAuthority HTTPS_AUTHORITY = AllowedAuthority.forBaseUri(URI.create("https://api.example.com/v3"));

    @Test
    public void testSameAuthorityIsAllowed()
    {
        URI paginated = URI.create("https://api.example.com/v3/items?page=2");
        assertThat(HTTPS_AUTHORITY.check(paginated))
                .isEqualTo(paginated);

        URI otherPath = URI.create("https://api.example.com/other");
        assertThat(HTTPS_AUTHORITY.check(otherPath))
                .isEqualTo(otherPath);
    }

    @Test
    public void testDefaultPortsAreEquivalent()
    {
        assertThat(HTTPS_AUTHORITY.check(URI.create("https://api.example.com:443/v3/items"))).isNotNull();
        assertThat(AllowedAuthority.forBaseUri(URI.create("https://api.example.com:443/"))
                .check(URI.create("https://api.example.com/v3/items")))
                .isNotNull();
    }

    @Test
    public void testHostIsComparedCaseInsensitively()
    {
        assertThat(HTTPS_AUTHORITY.check(URI.create("https://API.Example.COM/v3/items"))).isNotNull();
    }

    @Test
    public void testOtherHostIsRejected()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("http://169.254.169.254/latest/meta-data/")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request http://169.254.169.254:80")
                .hasMessageContaining("https://api.example.com:443");

        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("https://api.example.com.evil.test/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");
    }

    @Test
    public void testOtherPortIsRejected()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("https://api.example.com:8443/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");
    }

    @Test
    public void testSchemeDowngradeIsRejected()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("http://api.example.com/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");
    }

    @Test
    public void testSchemeUpgradeOnSameHostIsAllowed()
    {
        assertThat(AllowedAuthority.forBaseUri(URI.create("http://api.example.com/v3"))
                .check(URI.create("https://api.example.com/v3/items")))
                .isNotNull();
    }

    @Test
    public void testSchemeUpgradeOnSameExplicitPortIsAllowed()
    {
        assertThat(AllowedAuthority.forBaseUri(URI.create("http://api.example.com:8080/v3"))
                .check(URI.create("https://api.example.com:8080/v3/items")))
                .isNotNull();
    }

    @Test
    public void testSchemeUpgradeToAnotherPortIsRejected()
    {
        assertThatThrownBy(() -> AllowedAuthority.forBaseUri(URI.create("http://api.example.com:8080/v3"))
                .check(URI.create("https://api.example.com:8443/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");

        assertThatThrownBy(() -> AllowedAuthority.forBaseUri(URI.create("http://api.example.com:8080/v3"))
                .check(URI.create("https://api.example.com/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");
    }

    @Test
    public void testNonHttpSchemeIsRejected()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("file:///etc/passwd")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");

        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to request");
    }

    @Test
    public void testEmbeddedCredentialsAreRejected()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("https://user:pass@api.example.com/v3/items")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("embeds credentials");
    }

    @Test
    public void testRejectionMessageOmitsPathAndQuery()
    {
        assertThatThrownBy(() -> HTTPS_AUTHORITY.check(URI.create("https://evil.test/collect?api_key=s3cret")))
                .isInstanceOf(TrinoException.class)
                .hasMessageNotContaining("s3cret")
                .hasMessageNotContaining("api_key")
                .hasMessageNotContaining("/collect");
    }

    @Test
    public void testBaseUriWithoutHostIsRejected()
    {
        assertThatThrownBy(() -> AllowedAuthority.forBaseUri(URI.create("/relative/path")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("openapi.base-uri must be an absolute http or https URI with a host");
    }
}
