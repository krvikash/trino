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
package com.starburstdata.trino.plugin.internaltesting;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInternalTestingNodeOOM
{
    private enum NodeType
    {
        COORDINATOR("coordinator"),
        WORKER("worker");

        private final String value;

        NodeType(String value)
        {
            this.value = value;
        }
    }

    @Test
    void testOOMTriggeredOnWorker()
            throws Exception
    {
        testOOMTriggeredOnNode(NodeType.WORKER);
    }

    @Test
    void testOOMTriggeredOnCoordinator()
            throws Exception
    {
        testOOMTriggeredOnNode(NodeType.COORDINATOR);
    }

    void testOOMTriggeredOnNode(NodeType nodeType)
            throws Exception
    {
        ProcessBuilder processBuilder = new ProcessBuilder(
                ProcessHandle.current().info().command().orElseThrow(),
                "-Xmx512m",
                "--add-modules", "jdk.incubator.vector",
                "-XX:+ExitOnOutOfMemoryError",
                "-cp", System.getProperty("java.class.path"),
                OOMRunner.class.getName(),
                nodeType.value);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String output = new String(process.getInputStream().readAllBytes(), UTF_8);
        int exitCode = process.waitFor();

        assertThat(exitCode).isNotEqualTo(0);
        assertThat(output).contains("java.lang.OutOfMemoryError");
    }
}
