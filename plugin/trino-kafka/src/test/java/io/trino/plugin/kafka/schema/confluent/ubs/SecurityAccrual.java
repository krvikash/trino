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
package io.trino.plugin.kafka.schema.confluent.ubs;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SecurityAccrual(
        @JsonProperty("accrualKey") String accrualKey,
        @JsonProperty("accountIdentifier") String accountIdentifier,
        @JsonProperty("accountIdentifierType") String accountIdentifierType,
        @JsonProperty("accrualAmount") double accrualAmount,
        @JsonProperty("accrualBenchmarkRate") double accrualBenchmarkRate,
        @JsonProperty("accrualCurrency") String accrualCurrency,
        @JsonProperty("accrualDate") String accrualDate,
        @JsonProperty("accrualRate") double accrualRate,
        @JsonProperty("accrualSpread") double accrualSpread,
        @JsonProperty("accrualStatus") String accrualStatus,
        @JsonProperty("accrualType") String accrualType,
        @JsonProperty("dayCount") long dayCount,
        @JsonProperty("dayCountMethod") String dayCountMethod,
        @JsonProperty("fxRate") double fxRate,
        @JsonProperty("market") String market,
        @JsonProperty("payDate") String payDate,
        @JsonProperty("postingAccountIdentifier") String postingAccountIdentifier,
        @JsonProperty("postingAccountIdentifierType") String postingAccountIdentifierType,
        @JsonProperty("price") double price,
        @JsonProperty("priceType") String priceType,
        @JsonProperty("productTypeCode") String productTypeCode,
        @JsonProperty("securityIdentifier") String securityIdentifier,
        @JsonProperty("securityIdentifierType") String securityIdentifierType,
        @JsonProperty("securityStatus") String securityStatus,
        @JsonProperty("settledQuantity") double settledQuantity,
        @JsonProperty("settlementCurrency") String settlementCurrency,
        @JsonProperty("accrualId") String accrualId,
        @JsonProperty("submarket") String submarket,
        @JsonProperty("tradeIdentifier") long tradeIdentifier,
        @JsonProperty("valuationCurrency") String valuationCurrency) {}
