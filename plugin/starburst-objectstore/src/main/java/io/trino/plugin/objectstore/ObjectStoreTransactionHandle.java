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
package io.trino.plugin.objectstore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ObjectStoreTransactionHandle
        implements ConnectorTransactionHandle
{
    private final HiveTransactionHandle hiveHandle;
    private final HiveTransactionHandle icebergHandle;
    private final HiveTransactionHandle deltaHandle;
    private final HiveTransactionHandle hudiHandle;

    @JsonCreator
    public ObjectStoreTransactionHandle(
            HiveTransactionHandle hiveHandle,
            HiveTransactionHandle icebergHandle,
            HiveTransactionHandle deltaHandle,
            HiveTransactionHandle hudiHandle)
    {
        this.hiveHandle = requireNonNull(hiveHandle, "hiveHandle is null");
        this.icebergHandle = requireNonNull(icebergHandle, "icebergHandle is null");
        this.deltaHandle = requireNonNull(deltaHandle, "deltaHandle is null");
        this.hudiHandle = requireNonNull(hudiHandle, "hudiHandle is null");
    }

    @JsonProperty
    public HiveTransactionHandle getHiveHandle()
    {
        return hiveHandle;
    }

    @JsonProperty
    public HiveTransactionHandle getIcebergHandle()
    {
        return icebergHandle;
    }

    @JsonProperty
    public HiveTransactionHandle getDeltaHandle()
    {
        return deltaHandle;
    }

    @JsonProperty
    public HiveTransactionHandle getHudiHandle()
    {
        return hudiHandle;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectStoreTransactionHandle that = (ObjectStoreTransactionHandle) o;
        return hiveHandle.equals(that.hiveHandle) &&
                icebergHandle.equals(that.icebergHandle) &&
                deltaHandle.equals(that.deltaHandle) &&
                hudiHandle.equals(that.hudiHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hiveHandle, icebergHandle, deltaHandle, hudiHandle);
    }

    @Override
    public String toString()
    {
        // every transaction has all handles, so it doesn't matter which one we pick
        return hiveHandle.toString();
    }
}
