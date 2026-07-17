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
package io.trino.server.starburst;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class GalaxyImageConstants
{
    public static final String STARGATE_IMAGE_TAG = loadStargateVersion();
    public static final String STARGATE_DOCKER_REPO = "179619298502.dkr.ecr.us-east-1.amazonaws.com/galaxy/";

    private GalaxyImageConstants() {}

    private static String loadStargateVersion()
    {
        try {
            return Resources.toString(Resources.getResource("testing-stargate-version.txt"), UTF_8).trim();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
