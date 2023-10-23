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
package org.apache.doris.flink.tools.cdc.tidb;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.flink.util.Preconditions;

public class TidbType {
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String BIGINT = "BIGINT";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String REAL = "REAL";
    private static final String FLOAT = "FLOAT";
    private static final String DOUBLE = "DOUBLE";

    private static final String NUMERIC = "NUMERIC";
    private static final String DECIMAL = "DECIMAL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String ENUM = "ENUM";

    public static String toDorisType(String type, Integer length, Integer scale) {
        switch (type.toUpperCase()) {
            case BIT:
            case BOOLEAN:
            case BOOL:
                return DorisType.BOOLEAN;
            case TINYINT:
                return DorisType.TINYINT;
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return DorisType.SMALLINT;
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
            case YEAR:
                return DorisType.INT;
            case INT_UNSIGNED:
            case BIGINT:
                return DorisType.BIGINT;
            case BIGINT_UNSIGNED:
                return DorisType.LARGEINT;
            case FLOAT:
                return DorisType.FLOAT;
            case REAL:
                return DorisType.DOUBLE;
            case NUMERIC:

            case DECIMAL:
                return length != null && length <= 38
                        ? String.format("%s(%s,%s)", DorisType.DECIMAL_V3, length, scale != null && scale >= 0 ? scale : 0)
                        : DorisType.STRING;
            case DATE:
                return DorisType.DATE_V2;
            case DATETIME:
            case TIMESTAMP:
                return String.format("%s(%s)", DorisType.DATETIME_V2, Math.min(length == null ? 0 : length, 6));
            case CHAR:
            case VARCHAR:
                Preconditions.checkNotNull(length);
                return length * 3 > 65533 ? DorisType.STRING : String.format("%s(%s)", DorisType.VARCHAR, length * 3);
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case ENUM:
            case TIME:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BINARY:
            case VARBINARY:
                return DorisType.STRING;
            case JSON:
                return DorisType.JSONB;
            default:
                throw new UnsupportedOperationException("Unsupported TiDB Type: " + type);
        }

    }
}
