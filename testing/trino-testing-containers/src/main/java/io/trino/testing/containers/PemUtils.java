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
package io.trino.testing.containers;

import io.airlift.security.pem.PemReader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;

// copied from Galaxy repo
public final class PemUtils
{
    private PemUtils() {}

    /**
     * Usage: getCertFile("service") || getCertFile("stargate") || getCertFile("accesscontrol")
     */
    public static File getCertFile(String name)
    {
        File certFile = Path.of(System.getProperty("user.home"), "cert", name + ".pem").toFile();
        if (certFile.canRead()) {
            return certFile;
        }

        String envPemString = System.getenv(format("GALAXY_%s_PEM", name.toUpperCase(Locale.ROOT)));
        if (envPemString != null && !envPemString.isBlank()) {
            try {
                FileAttribute<Set<PosixFilePermission>> readWrite = asFileAttribute(PosixFilePermissions.fromString("rw-rw-rw-"));
                Path dst = Files.createTempFile(format("galaxy_%s_", name), ".pem", readWrite);
                Files.writeString(dst, envPemString, UTF_8);
                return dst.toFile();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed writing temporary certificate file", e);
            }
        }

        throw new RuntimeException("Cannot find certificate: " + name);
    }

    public static String getHostNameFromPem(File pem)
    {
        try {
            verify(pem.canRead(), "Can not read certificate pem: %s", pem);
            KeyStore keyStore = PemReader.loadKeyStore(pem, pem, Optional.empty());
            X509Certificate certificate = (X509Certificate) keyStore.getCertificate("key");

            return certificate.getSubjectAlternativeNames().stream()
                    .map(san -> (String) san.get(1))
                    .filter(name -> !name.startsWith("*."))
                    .filter(PemUtils::isThisMyHost)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Certificate does not contain a non-wildcard SAN"));
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isThisMyHost(String name)
    {
        try {
            InetAddress address = InetAddress.getByName(name);
            if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
                return true;
            }
            return NetworkInterface.getByInetAddress(address) != null;
        }
        catch (SocketException | UnknownHostException e) {
            return false;
        }
    }
}
