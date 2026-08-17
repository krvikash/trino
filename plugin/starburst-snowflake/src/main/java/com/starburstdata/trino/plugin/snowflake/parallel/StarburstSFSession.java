/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.resultset.QueryStatus;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.config.SFClientConfig;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.OCSPMode;
import net.snowflake.client.internal.core.QueryContextDTO;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.internal.core.SFSessionProperty;
import net.snowflake.client.internal.jdbc.SFConnectionHandler;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import net.snowflake.client.internal.jdbc.telemetry.Telemetry;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

// To create SFSession required by StarburstDataConversionContext which explicitly implements only required methods
// so that we can catch any unintended behaviour with the Snowflake driver bumps.
public class StarburstSFSession
        extends SFSession
{
    public StarburstSFSession()
    {
        super();
    }

    @Override
    public boolean isSafeToClose()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addQueryToActiveQueryList(String queryID)
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public QueryStatus getQueryStatus(String queryID)
            throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSFSessionProperty(String propertyName, Object propertyValue)
            throws SFException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void overrideConsoleHandlerWhenNecessary()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containProperty(String key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void open()
            throws SFException, SnowflakeSQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
            throws SFException, SnowflakeSQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void callHeartBeat(int timeout)
            throws Exception, SFException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void startHeartbeatForThisSession()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void stopHeartbeatForThisSession()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void heartbeat()
            throws SFException, SQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInjectSocketTimeout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInjectSocketTimeout(int injectSocketTimeout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNetworkTimeoutInMilli()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getAuthTimeout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHttpClientSocketTimeout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHttpClientConnectionTimeout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInjectClientPause()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxHttpRetries()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInjectClientPause(int injectClientPause)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int getAndIncrementSequenceId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getEnableCombineDescribe()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnableCombineDescribe(boolean enable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Telemetry getTelemetryClient()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeTelemetryClient()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getIdToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMfaToken()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SnowflakeConnectString getSnowflakeConnectionString()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSnowflakeConnectionString(SnowflakeConnectString connStr)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DriverPropertyInfo> checkProperties()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAsyncSession()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setQueryContext(String queryContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryContextDTO getQueryContextDTO()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SFClientConfig getSfClientConfig()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSfClientConfig(SFClientConfig sfClientConfig)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMemoryLimitForTesting(long memLimit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMemoryLimitForTesting()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getClientInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCommonParameters(Map<String, Object> parameters)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getCommonParameters()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientInfo(String name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSessionId(String sessionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSfSQLMode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSfSQLMode(boolean sfSQLMode)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDatabaseVersion()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDatabaseVersion(String databaseVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDatabaseMajorVersion()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDatabaseMajorVersion(int databaseMajorVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDatabaseMinorVersion()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDatabaseMinorVersion(int databaseMinorVersion)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getPreparedStatementLogging()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPreparedStatementLogging(boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getInjectFileUploadFailure()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInjectFileUploadFailure(String fileToFail)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SnowflakeType getTimestampMappedType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestampMappedType(SnowflakeType timestampMappedType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isResultColumnCaseInsensitive()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setResultColumnCaseInsensitive(boolean resultColumnCaseInsensitive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isJdbcTreatDecimalAsInt()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJdbcTreatDecimalAsInt(boolean jdbcTreatDecimalAsInt)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isJdbcArrowTreatDecimalAsInt()
    {
        return super.isJdbcArrowTreatDecimalAsInt();
    }

    @Override
    public void setJdbcArrowTreatDecimalAsInt(boolean jdbcArrowTreatDecimalAsInt)
    {
        super.setJdbcArrowTreatDecimalAsInt(jdbcArrowTreatDecimalAsInt);
    }

    @Override
    public String getServerUrl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStringQuoted()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addProperty(SFSessionProperty sfSessionProperty, Object propertyValue)
            throws SFException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addProperty(String propertyName, Object propertyValue)
            throws SFException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<SFSessionProperty, Object> getConnectionPropertiesMap()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpClientSettingsKey getHttpClientKey()
            throws SnowflakeSQLException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unsetInvalidProxyHostAndPort()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OCSPMode getOCSPMode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getQueryTimeout()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUser()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getUrl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInjectWaitInPut()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMetadataRequestUseSessionDatabase()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadataRequestUseSessionDatabase(boolean enabled)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMetadataRequestUseConnectionCtx()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMetadataRequestUseConnectionCtx(boolean enabled)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInjectedDelay(int injectedDelay)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getTreatNTZAsUTC()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTreatNTZAsUTC(boolean treatNTZAsUTC)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getEnableHeartbeat()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnableHeartbeat(boolean enableHeartbeat)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHeartbeatFrequency(int frequency)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHeartbeatFrequency()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAutoCommit()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getFormatDateWithTimezone()
    {
        return super.getFormatDateWithTimezone();
    }

    @Override
    public void setFormatDateWithTimezone(boolean formatDateWithTimezone)
    {
        super.setFormatDateWithTimezone(formatDateWithTimezone);
    }

    @Override
    public boolean getUseSessionTimezone()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getDefaultFormatDateWithTimezone()
    {
        return super.getDefaultFormatDateWithTimezone();
    }

    @Override
    public void setUseSessionTimezone(boolean useSessionTimezone)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultFormatDateWithTimezone(boolean defaultFormatDateWithTimezone)
    {
        super.setDefaultFormatDateWithTimezone(defaultFormatDateWithTimezone);
    }

    @Override
    public boolean getGetDateUseNullTimezone()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setGetDateUseNullTimezone(boolean getDateUseNullTimezone)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClientTelemetryEnabled()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientTelemetryEnabled(boolean clientTelemetryEnabled)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getArrayBindStageThreshold()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArrayBindStageThreshold(int arrayBindStageThreshold)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getStoreTemporaryCredential()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStoreTemporaryCredential(boolean storeTemporaryCredential)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setServiceName(String serviceName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnableConservativeMemoryUsage(boolean enableConservativeMemoryUsage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConservativeMemoryUsageEnabled()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getConservativeMemoryAdjustStep()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConservativeMemoryAdjustStep(int conservativeMemoryAdjustStep)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClientMemoryLimit()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientMemoryLimit(int clientMemoryLimit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getQueryContextCacheSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setQueryContextCacheSize(int queryContextCacheSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getJdbcEnablePutGet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJdbcEnablePutGet(boolean jdbcEnablePutGet)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getEnablePutGet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setEnablePutGet(boolean enablePutGet)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getEnablePatternSearch()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setEnablePatternSearch(boolean enablePatternSearch)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getDisableGcsDefaultCredentials()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDisableGcsDefaultCredentials(boolean disableGcsDefaultCredentials)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClientResultChunkSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientResultChunkSize(int clientResultChunkSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getOtherParameter(String key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOtherParameter(String key, Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClientPrefetchThreads()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientPrefetchThreads(int clientPrefetchThreads)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getValidateDefaultParameters()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValidateDefaultParameters(boolean validateDefaultParameters)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDatabase()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDatabase(String database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchema()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRole()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRole(String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWarehouse()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWarehouse(String warehouse)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUseRegionalS3EndpointsForPresignedURL(boolean regionalS3Endpoint)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getUseRegionalS3EndpointsForPresignedURL()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getArrayBindStage()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setArrayBindStage(String arrayBindStage)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSessionPropertyByKey(String propertyName, Object propertyValue)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getSessionPropertyByKey(String propertyName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SFException> getSqlWarnings()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearSqlWarnings()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SFConnectionHandler getSfConnectionHandler()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getEnableReturnTimestampWithTimeZone()
    {
        throw new UnsupportedOperationException();
    }
}
