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
import io.confluent.kafka.schemaregistry.annotations.Schema;

// see schema1 in the MockData_JsonSchema.pdf from https://starburstdata.atlassian.net/browse/PI-1411
@Schema(value = """
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "definitions": {
                    "AccountIdentifierType": {
                        "type": "string",
                        "enum": [
                            "CCONSOL",
                            "WRAPPER"
                        ]
                    },
                    "AccrualType": {
                        "type": "string",
                        "enum": [
                            "MI",
                            "FEE",
                            "SCI",
                            "ABB",
                            "AXE",
                            "TKT",
                            "DYN",
                            "CTY",
                            "ALL",
                            "LNINT",
                            "PTH"
                        ]
                    },
                    "PriceType": {
                        "type": "string",
                        "enum": [
                            "COB",
                            "MTM",
                            "COST",
                            "MV",
                            "DLTA"
                        ]
                    },
                    "ProductTypeCode": {
                        "type": "string",
                        "enum": [
                            "PB",
                            "SWAP",
                            "ALL"
                        ]
                    },
                    "SecurityAccrual": {
                        "type": "object",
                        "properties": {
                            "accrualKey": {
                                "type": "string",
                                "description": "Accrual Key"
                            },
                            "accountIdentifier": {
                                "type": "string",
                                "description": "Account Identifier"
                            },
                            "accountIdentifierType": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/AccountIdentifierType"
                                    },
                                    {
                                        "description": "Account Identifier Type"
                                    }
                                ]
                            },
                            "accrualAmount": {
                                "type": "number",
                                "description": "Accrual Amount"
                            },
                            "accrualBenchmarkRate": {
                                "type": "number",
                                "description": "Benchmark"
                            },
                            "accrualCurrency": {
                                "type": "string",
                                "description": "Accrual Currency"
                            },
                            "accrualDate": {
                                "type": "string",
                                "format": "date",
                                "description": "Accrual Date"
                            },
                            "accrualRate": {
                                "type": "number",
                                "description": "Accrual Rate"
                            },
                            "accrualSpread": {
                                "type": "number",
                                "description": "Spread"
                            },
                            "accrualStatus": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/StatusType"
                                    },
                                    {
                                        "description": "Accrual Status"
                                    }
                                ]
                            },
                            "accrualType": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/AccrualType"
                                    },
                                    {
                                        "description": "Accrual Type Code"
                                    }
                                ]
                            },
                            "dayCount": {
                                "type": "integer",
                                "description": "Day Count"
                            },
                            "dayCountMethod": {
                                "type": "string",
                                "description": "Day Count Method"
                            },
                            "fxRate": {
                                "type": "number",
                                "description": "Fx Currency"
                            },
                            "market": {
                                "type": "string",
                                "description": "Market"
                            },
                            "payDate": {
                                "type": "string",
                                "format": "date",
                                "description": "Pay Date"
                            },
                            "postingAccountIdentifier": {
                                "type": "string",
                                "description": "Posting Account Identifier"
                            },
                            "postingAccountIdentifierType": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/AccountIdentifierType"
                                    },
                                    {
                                        "description": "Posting Account Identifier Type"
                                    }
                                ]
                            },
                            "price": {
                                "type": "number",
                                "description": "Price"
                            },
                            "priceType": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/PriceType"
                                    },
                                    {
                                        "description": "Price Type"
                                    }
                                ]
                            },
                            "productTypeCode": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/ProductTypeCode"
                                    },
                                    {
                                        "description": "Product Type Code"
                                    }
                                ]
                            },
                            "securityIdentifier": {
                                "type": "string",
                                "description": "Security Identifier"
                            },
                            "securityIdentifierType": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/SecurityIdentifierType"
                                    },
                                    {
                                        "description": "Security Identifier Type"
                                    }
                                ]
                            },
                            "securityStatus": {
                                "allOf": [
                                    {
                                        "$ref": "#/definitions/SecurityStatus"
                                    },
                                    {
                                        "description": "Security Status"
                                    }
                                ]
                            },
                            "settledQuantity": {
                                "type": "number",
                                "description": "Settled Quantity"
                            },
                            "settlementCurrency": {
                                "type": "string",
                                "description": "Settlement Currency"
                            },
                            "accrualId": {
                                "type": "string",
                                "description": "Unique identifier per product"
                            },
                            "submarket": {
                                "type": "string",
                                "description": "Sub Market"
                            },
                            "tradeIdentifier": {
                                "type": "integer",
                                "description": "Trade Identifier"
                            },
                            "valuationCurrency": {
                                "type": "string",
                                "description": "Valuation Currency"
                            }
                        },
                        "required": [
                            "accountIdentifier",
                            "accountIdentifierType",
                            "accrualRate",
                            "accrualAmount",
                            "accrualDate",
                            "accrualStatus",
                            "accrualType",
                            "productTypeCode",
                            "accrualId"
                        ],
                        "description": "Security Accrual Data",
                        "additionalProperties": false
                    },
                    "SecurityIdentifierType": {
                        "type": "string",
                        "enum": [
                            "SEDOL",
                            "ISIN",
                            "CUSIP",
                            "RIC",
                            "OSITICKER"
                        ]
                    },
                    "SecurityStatus": {
                        "type": "string",
                        "enum": [
                            "HTB",
                            "GC"
                        ]
                    },
                    "SourceType": {
                        "type": "string",
                        "enum": [
                            "SBE",
                            "SABRE"
                        ]
                    },
                    "StatusType": {
                        "type": "string",
                        "enum": [
                            "L",
                            "C",
                            "U"
                        ]
                    }
                },
                "type": "object",
                "javaType": "io.trino.plugin.kafka.schema.confluent.ubs.UbsMockRecord",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Type of message"
                    },
                    "source": {
                        "allOf": [
                            {
                                "$ref": "#/definitions/SourceType"
                            },
                            {
                                "description": "Source System"
                            }
                        ]
                    },
                    "time": {
                        "type": "integer",
                        "description": "System Time in millis"
                    },
                    "data": {
                        "allOf": [
                            {
                                "$ref": "#/definitions/SecurityAccrual"
                            },
                            {
                                "description": "Security Accrual Data"
                            }
                        ]
                    }
                },
                "required": [
                    "type",
                    "data",
                    "source",
                    "time"
                ],
                "description": "Security Accrual Envelope",
                "additionalProperties": false
            }
            """, refs = {})
public record UbsMockSecurityAccrualCommand(@JsonProperty("type") String type, @JsonProperty("source") String source, @JsonProperty("time") long time, @JsonProperty("data") SecurityAccrual data)
{}
