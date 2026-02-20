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
package com.starburstdata.plugin.kdb;

import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;

public final class KdbContainer
        implements Closeable
{
    private static final int KDB_PORT = 5000;

    private final GenericContainer<?> container;

    public KdbContainer()
    {
        // Note: The expiry date for the license is: 2027-04-28
        //noinspection resource
        container = new GenericContainer<>("843985043183.dkr.ecr.us-east-1.amazonaws.com/testing/kdb:135")
                .withExposedPorts(KDB_PORT);
        container.start();
    }

    public String host()
    {
        return container.getHost();
    }

    public int port()
    {
        return container.getMappedPort(KDB_PORT);
    }

    public KdbClient client()
    {
        return new KdbClient(new KdbConnectionFactory(new KdbConfig().setHost(host()).setPort(port()), new KdbCredentialConfig()));
    }

    @Override
    public void close()
    {
        container.stop();
    }
}
