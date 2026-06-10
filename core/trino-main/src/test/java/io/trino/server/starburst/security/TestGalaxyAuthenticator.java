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
package io.trino.server.starburst.security;

import io.jsonwebtoken.JwtBuilder;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.trino.server.security.AuthenticationException;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.RequestBodyHashing;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.starburst.security.AbstractGalaxyAuthenticatorController.REQUEST_EXPIRATION_CLAIM;
import static io.trino.server.starburst.security.GalaxyAuthenticationHelper.parseClaimsWithoutValidation;
import static io.trino.server.starburst.security.GalaxyIdentity.PORTAL_IDENTITY_TYPE;
import static io.trino.server.starburst.security.GalaxyIdentity.createPrincipalString;
import static io.trino.server.starburst.security.GalaxyIdentity.toGalaxyIdentityType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class TestGalaxyAuthenticator
{
    protected static final String TOKEN_ISSUER = "https://issuer.example.com";
    protected static final String ACCOUNT_ID = "a-12345678";
    protected static final String USER_ID = "u-1234567890";
    protected static final String ROLE_ID = "r-9876543252";
    protected static final String GALAXY_IDENTITY = createPrincipalString(new AccountId(ACCOUNT_ID), new UserId(USER_ID), new RoleId(ROLE_ID));

    static Date notExpired()
    {
        return Date.from(ZonedDateTime.now().plusMinutes(5).toInstant());
    }

    static Date requestNotExpired()
    {
        return Date.from(ZonedDateTime.now().plusSeconds(60).toInstant());
    }

    @FunctionalInterface
    interface TestAuthenticator
    {
        Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
                throws AuthenticationException;
    }

    protected void test(String subject, KeyPair keyPair, TestAuthenticator authenticator)
            throws Exception
    {
        test(subject, keyPair, authenticator, TOKEN_ISSUER, PORTAL_IDENTITY_TYPE);
    }

    protected void test(String subject, KeyPair keyPair, TestAuthenticator authenticator, String issuer, String identityType)
            throws Exception
    {
        assertThat(authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate()), Optional.empty()))
                .satisfies(identity -> assertThat(identity.getUser()).isEqualTo("username"))
                .satisfies(identity -> assertThat(identity.getPrincipal().orElseThrow().toString()).isEqualTo(GALAXY_IDENTITY));

        // Test explicit issuer assignment
        assertThat(authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("good"), issuer, Optional.of(identityType)), Optional.empty()))
                .satisfies(identity -> assertThat(identity.getUser()).isEqualTo("username"))
                .satisfies(identity -> assertThat(identity.getPrincipal().orElseThrow().toString()).isEqualTo(GALAXY_IDENTITY))
                .satisfies(identity -> assertThat(identity.getExtraCredentials().get("identityType")).isEqualTo(toGalaxyIdentityType(identityType).name()));

        RequestBodyHashing requestBodyHashing = new RequestBodyHashing("good", "statement_hash");
        assertThat(authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("good")), Optional.of(requestBodyHashing)))
                .satisfies(identity -> assertThat(identity.getUser()).isEqualTo("username"))
                .satisfies(identity -> assertThat(identity.getPrincipal().orElseThrow().toString()).isEqualTo(GALAXY_IDENTITY));

        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", "unknown", subject, keyPair.getPrivate()), Optional.empty()))
                .isInstanceOf(AuthenticationException.class)
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));

        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, "unknown", keyPair.getPrivate()), Optional.empty()))
                .isInstanceOf(AuthenticationException.class)
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));

        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, "unknown", generateKeyPair().getPrivate()), Optional.empty()))
                .isInstanceOf(AuthenticationException.class)
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));

        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("good"), "badIssuer", Optional.of(PORTAL_IDENTITY_TYPE)), Optional.of(requestBodyHashing)))
                .isInstanceOf(AuthenticationException.class)
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));

        Date expired = new Date(System.currentTimeMillis() - 1000);
        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), expired, requestNotExpired(), Optional.empty()), Optional.empty()))
                .isInstanceOf(AuthenticationException.class)
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));

        Date requestExpired = Date.from(ZonedDateTime.now().minusSeconds(60).toInstant());
        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), notExpired(), requestExpired, Optional.of("good")), Optional.of(requestBodyHashing)))
                .isInstanceOfSatisfying(AuthenticationException.class, exception -> assertThat(exception.getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Token expired");

        assertThatThrownBy(() -> authenticator.authenticate(generateJwt("username", ACCOUNT_ID, subject, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("bad")), Optional.of(requestBodyHashing)))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Request body hash does not match")
                .satisfies(exception -> assertThat(((AuthenticationException) exception).getAuthenticateHeader()).isEqualTo(Optional.of("Galaxy")));
    }

    @Test
    public void testParseClaimsWithoutValidation()
    {
        Optional<String> claims = parseClaimsWithoutValidation(
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." +
                        "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ." +
                        "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c");
        assertThat(claims).contains("{sub=1234567890, name=John Doe, iat=1516239022}");
    }

    static KeyPair generateKeyPair()
            throws NoSuchAlgorithmException
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(4096);
        return generator.generateKeyPair();
    }

    protected static String generateJwt(String username, String accountId, String subject, PrivateKey privateKey)
    {
        return generateJwt(username, accountId, subject, privateKey, notExpired(), requestNotExpired(), Optional.empty());
    }

    protected static String generateJwt(String username, String accountId, String deploymentId, PrivateKey privateKey, Date expiration, Date requestExpiration, Optional<String> statementHash)
    {
        return generateJwt(username, accountId, deploymentId, privateKey, expiration, requestExpiration, statementHash, TOKEN_ISSUER, Optional.of(PORTAL_IDENTITY_TYPE));
    }

    protected static String generateJwt(String username, String accountId, String deploymentId, PrivateKey privateKey, Date expiration, Date requestExpiration, Optional<String> statementHash, String issuer, Optional<String> identityType)
    {
        JwtBuilder builder = newJwtBuilder()
                .signWith(privateKey)
                .setIssuer(issuer)
                .setAudience(accountId)
                .setSubject(deploymentId)
                .setExpiration(expiration)
                .claim("username", username)
                .claim("user_id", USER_ID)
                .claim("role_id", ROLE_ID)
                .claim("role_name", "roletest")
                .claim(REQUEST_EXPIRATION_CLAIM, requestExpiration)
                .claim("statement_hash", statementHash.orElse(null));
        if (identityType.isPresent()) {
            builder.claim("identity_type", identityType.get());
        }
        return builder.compact();
    }
}
