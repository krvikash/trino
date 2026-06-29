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
package io.starburst.materialization.metastore.server.db;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class TestingDbConfig
{
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;

    @NotNull
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("metastore.jdbc.url")
    @ConfigDescription("JDBC URL of the materialization metastore database")
    public TestingDbConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getJdbcUser()
    {
        return jdbcUser;
    }

    @Config("metastore.jdbc.user")
    @ConfigDescription("Database user name")
    public TestingDbConfig setJdbcUser(String jdbcUser)
    {
        this.jdbcUser = jdbcUser;
        return this;
    }

    public String getJdbcPassword()
    {
        return jdbcPassword;
    }

    @ConfigSecuritySensitive
    @Config("metastore.jdbc.password")
    @ConfigDescription("Database password")
    public TestingDbConfig setJdbcPassword(String jdbcPassword)
    {
        this.jdbcPassword = jdbcPassword;
        return this;
    }

    public boolean isPostgreSqlUrl()
    {
        return jdbcUrl.startsWith("jdbc:postgresql");
    }

    public boolean isOracleUrl()
    {
        return jdbcUrl.startsWith("jdbc:oracle");
    }

    public boolean isMySqlUrl()
    {
        return jdbcUrl.startsWith("jdbc:mysql");
    }
}
