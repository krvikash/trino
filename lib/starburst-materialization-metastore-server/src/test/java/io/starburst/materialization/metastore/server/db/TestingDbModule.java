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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mysql.cj.jdbc.MysqlDataSource;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import oracle.jdbc.pool.OracleDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;

import java.sql.SQLException;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class TestingDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(TestingDbConfig.class);
        TestingDbConfig testingDbConfig = buildConfigObject(TestingDbConfig.class);
        if (testingDbConfig.isPostgreSqlUrl()) {
            install(new PostgreSqlModule());
        }

        if (testingDbConfig.isOracleUrl()) {
            install(new OracleModule());
        }

        if (testingDbConfig.isMySqlUrl()) {
            install(new MySqlModule());
        }
        binder.bind(Key.get(DataSource.class, ForMaterializationMetastore.class)).to(DataSource.class);
        binder.bind(Key.get(Jdbi.class, ForMaterializationMetastore.class)).to(Jdbi.class);
    }

    @Singleton
    @Provides
    public Jdbi jdbi(@ForMaterializationMetastore DataSource dataSource)
    {
        Jdbi jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }

    private static class PostgreSqlModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(DatabaseType.class).toInstance(DatabaseType.POSTGRESQL);
        }

        @Singleton
        @Provides
        public DataSource createConnectionFactory(TestingDbConfig config)
        {
            PGSimpleDataSource postgreSqlDataSource = new PGSimpleDataSource();
            postgreSqlDataSource.setURL(config.getJdbcUrl());
            postgreSqlDataSource.setUser(config.getJdbcUser());
            postgreSqlDataSource.setPassword(config.getJdbcPassword());
            return postgreSqlDataSource;
        }
    }

    private static class MySqlModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(DatabaseType.class).toInstance(DatabaseType.MYSQL);
        }

        @Singleton
        @Provides
        public DataSource createConnectionFactory(TestingDbConfig config)
        {
            MysqlDataSource mySqlDataSource = new MysqlDataSource();
            mySqlDataSource.setURL(config.getJdbcUrl());
            mySqlDataSource.setUser(config.getJdbcUser());
            mySqlDataSource.setPassword(config.getJdbcPassword());
            return mySqlDataSource;
        }
    }

    private static class OracleModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            binder.bind(DatabaseType.class).toInstance(DatabaseType.ORACLE);
        }

        @Singleton
        @Provides
        public DataSource createConnectionFactory(TestingDbConfig config)
                throws SQLException
        {
            OracleDataSource oracleDataSource = new OracleDataSource();
            oracleDataSource.setURL(config.getJdbcUrl());
            oracleDataSource.setUser(config.getJdbcUser());
            oracleDataSource.setPassword(config.getJdbcPassword());
            return oracleDataSource;
        }
    }
}
