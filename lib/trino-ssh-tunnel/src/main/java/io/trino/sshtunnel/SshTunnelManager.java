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
package io.trino.sshtunnel;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SshTunnelManager
{
    private static final Logger log = Logger.get(SshTunnelManager.class);
    private static final int SSH_CONNECT_TIMEOUT_MS = 30_000;

    // Use static caches to retain state because socket factories are unfortunately recreated for each socket
    private static final LoadingCache<SshTunnelProperties, SshTunnelManager> SSH_TUNNEL_CACHE =
            buildNonEvictableCache(CacheBuilder.newBuilder(), CacheLoader.from(SshTunnelManager::new));

    private final HostAndPort sshServer;
    private final String sshUser;
    private final Duration reconnectCheckInterval;

    private final JSch ssh = new JSch();
    private final LoadingCache<HostAndPort, Tunnel> tunnelCache = buildNonEvictableCache(CacheBuilder.newBuilder(), new CacheLoader<>()
    {
        @Override
        public Tunnel load(HostAndPort target)
        {
            Supplier<Session> sessionFactory = () -> {
                try {
                    return ssh.getSession(sshUser, sshServer.getHost(), sshServer.getPortOrDefault(22));
                }
                catch (JSchException e) {
                    throw new RuntimeException(e);
                }
            };
            return new Tunnel(sessionFactory, target, reconnectCheckInterval);
        }
    });

    public static SshTunnelManager getCached(SshTunnelProperties properties)
    {
        return SSH_TUNNEL_CACHE.getUnchecked(properties);
    }

    public SshTunnelManager(SshTunnelProperties properties)
    {
        this(properties.getServer(), properties.getUser(), properties.getPrivateKey(), properties.getReconnectCheckInterval());
    }

    public SshTunnelManager(HostAndPort sshServer, String sshUser, String privateKey, Duration reconnectCheckInterval)
    {
        this.sshServer = requireNonNull(sshServer, "sshServer is null");
        this.sshUser = requireNonNull(sshUser, "sshUser is null");
        this.reconnectCheckInterval = requireNonNull(reconnectCheckInterval, "reconnectCheckInterval is null");
        try {
            ssh.addIdentity(sshServer + "-" + sshUser, privateKey.getBytes(UTF_8), null, null);
        }
        catch (JSchException e) {
            throw new IllegalArgumentException("Invalid private SSH key", e);
        }
    }

    public HostAndPort getSshServer()
    {
        return sshServer;
    }

    public Tunnel getOrCreateTunnel(HostAndPort target)
    {
        Tunnel tunnel = tunnelCache.getUnchecked(target);
        tunnel.reconnectIfNecessary();
        return tunnel;
    }

    public static class Tunnel
    {
        private final Supplier<Session> sessionFactory;
        private final HostAndPort target;
        private final long reconnectCheckIntervalMs;

        @GuardedBy("this")
        private Session session;

        @GuardedBy("this")
        private final Stopwatch timeSinceLastSuccessfulCheck = Stopwatch.createUnstarted();

        private volatile int localPort;

        private Tunnel(Supplier<Session> sessionFactory, HostAndPort target, Duration reconnectCheckInterval)
        {
            this.sessionFactory = requireNonNull(sessionFactory, "sessionFactory is null");
            this.target = requireNonNull(target, "target is null");
            checkArgument(target.hasPort(), "target must have a port");
            reconnectCheckIntervalMs = requireNonNull(reconnectCheckInterval, "reconnectCheckInterval is null").toMillis();
            reconnect();
        }

        private synchronized void reconnectIfNecessary()
        {
            if (!shouldCheckReconnect() || isConnected()) {
                return;
            }

            reconnect();
        }

        private synchronized boolean shouldCheckReconnect()
        {
            checkState(Thread.holdsLock(this));
            return !timeSinceLastSuccessfulCheck.isRunning() || timeSinceLastSuccessfulCheck.elapsed(MILLISECONDS) >= reconnectCheckIntervalMs;
        }

        // From initial testing, this can take ~200ms
        private synchronized boolean isConnected()
        {
            checkState(Thread.holdsLock(this));

            if (!session.isConnected()) {
                return false;
            }

            try {
                testCommand(session);
                timeSinceLastSuccessfulCheck.reset().start();
                return true;
            }
            catch (JSchException e) {
                return false;
            }
        }

        private static void testCommand(Session session)
                throws JSchException
        {
            long startNs = System.nanoTime();

            // https://stackoverflow.com/questions/16127200/jsch-how-to-keep-the-session-alive-and-up
            ChannelExec exec = (ChannelExec) session.openChannel("exec");
            exec.setCommand("true");
            exec.setInputStream(null); // Jsch will try to create an unneeded thread if this is non-null
            exec.connect(SSH_CONNECT_TIMEOUT_MS);
            exec.disconnect();

            log.debug("Time to test SSH tunnel liveness: %s", Duration.nanosSince(startNs));
        }

        // From initial testing, this can take ~10s
        private synchronized void reconnect()
        {
            long startNs = System.nanoTime();

            // Session can be null in the constructor
            if (nonNull(session)) {
                session.disconnect();
            }
            session = sessionFactory.get();

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            config.put("ConnectionAttempts", "3");
            session.setConfig(config);
            try {
                session.setServerAliveInterval(10_000); // send ping every 10 seconds
                session.setServerAliveCountMax(6); // kill connection after missing 6 consecutive responses
                session.connect(SSH_CONNECT_TIMEOUT_MS);
                localPort = session.setPortForwardingL(0, target.getHost(), target.getPort());
            }
            catch (JSchException e) {
                throw new RuntimeException("Failed to connect SSH tunnel", e);
            }
            timeSinceLastSuccessfulCheck.reset().start();

            log.info("Connecting SSH tunnel took %s", Duration.nanosSince(startNs));
        }

        public int getLocalTunnelPort()
        {
            return localPort;
        }
    }
}
