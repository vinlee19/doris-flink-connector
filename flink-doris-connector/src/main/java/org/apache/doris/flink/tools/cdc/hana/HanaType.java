// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.tools.cdc.hana;

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.DorisType;

public class HanaType {

    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String INTEGER = "INTEGER";
    private static final String BIGINT = "BIGINT";
    private static final String SMALLDECIMAL = "SMALLDECIMAL";
    private static final String DECIMAL = "DECIMAL";
    private static final String REAL = "REAL";
    private static final String DOUBLE = "DOUBLE";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String SECONDDATE = "SECONDDATE";
    private static final String DATE = "DATE";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String CHAR = "CHAR";
    private static final String NCHAR = "NCHAR";
    private static final String TIME = "TIME";
    private static final String VARCHAR = "VARCHAR";
    private static final String NVARCHAR = "NVARCHAR";
    private static final String ALPHANUM = "ALPHANUM";
    private static final String SHORTTEXT = "SHORTTEXT";
    private static final String CLOB = "CLOB";
    private static final String NCLOB = "NCLOB";
    private static final String TEXT = "TEXT";
    private static final String JSON = "JSON";
    private static final String BINTEXT = "BINTEXT";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String BLOB = "BLOB";
    private static final String ST_GEOMETRY = "ST_GEOMETRY";
    private static final String ST_POINT = "ST_POINT";

    public static String toDorisType(String type, Integer precision, Integer scale) {
        switch (type) {
            case TINYINT:
                return DorisType.TINYINT;
            case SMALLINT:
                return DorisType.SMALLINT;
            case INTEGER:
                return DorisType.INT;
            case BIGINT:
                return DorisType.BIGINT;
            case SMALLDECIMAL:
            case DECIMAL:
                {
                    // In SAP HANA, when you create a table with the DECIMAL data type and do not
                    // specify precision or scale, the default precision is 34 and the scale is
                    // null.
                    // We should not convert this to Doris DOUBLE type due to potential precision
                    // loss.
                    if (scale == null) {
                        scale = 0;
                    }

                    if (precision == null) {
                        precision = 0;
                    }
                    return precision <= 38
                            ? String.format(
                                    "%s(%s,%s)",
                                    DorisType.DECIMAL_V3, precision, scale >= 0 ? scale : 0)
                            : DorisType.STRING;
                }
            case REAL:
                return DorisType.FLOAT;
            case DOUBLE:
                return DorisType.DOUBLE;
            case TIMESTAMP:
                return String.format(
                        "%s(%s)", DorisType.DATETIME_V2, Math.min(scale == null ? 0 : scale, 6));
            case SECONDDATE:
                // SECONDDATE with second precision
                return String.format("%s(%s)", DorisType.DATETIME_V2, 0);
            case DATE:
                return DorisType.DATE_V2;
            case BOOLEAN:
                return DorisType.BOOLEAN;
            case CHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? DorisType.STRING
                        : String.format("%s(%s)", DorisType.VARCHAR, precision * 3);
            case TIME:
            case ALPHANUM:
            case SHORTTEXT:
            case CLOB:
            case NCLOB:
            case TEXT:
            case BINTEXT:
            case BINARY:
            case VARBINARY:
                return DorisType.STRING;
            case JSON:
                return DorisType.JSON;
            case BLOB:
            case ST_GEOMETRY:
            case ST_POINT:
            default:
                throw new UnsupportedOperationException("Unsupported SAP HANA Type: " + type);
        }
    }
}
