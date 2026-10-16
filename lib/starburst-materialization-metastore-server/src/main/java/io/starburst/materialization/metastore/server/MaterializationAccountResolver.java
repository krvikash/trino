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
package io.starburst.materialization.metastore.server;

import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.stargate.id.AccountId;

/**
 * Maps a metastore tenant to the account that owns it. Bound by the deployment: SEP has a single
 * account, Galaxy reads it from its metastores table. The server resolves the account itself rather
 * than trusting the client, which may not send it.
 */
public interface MaterializationAccountResolver
{
    AccountId accountId(MetastoreId metastoreId);
}
