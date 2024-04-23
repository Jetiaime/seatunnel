/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.table.catalog.TablePath;

public class ClickhouseCatalogUtil {
    public static final String ALL_DATABASE_QUERY = "SHOW DATABASES";

    public static final String DATABASES_QUERY_WITH_IDENTIFIER_QUERY = "SELECT name FROM system.databases WHERE name = :database";

    public static final String TABLES_QUERY_WITH_DATABASE_QUERY = "SELECT name FROM system.tables WHERE database = :database";

    public static final String TABLES_QUERY_WITH_IDENTIFIER_QUERY = "SELECT name FROM system.tables WHERE database = :database AND name = :name";

    public static final String TABLE_INFO_QUERY =
            "SELECT" +
            "    sc.name, sc.is_in_partition_key, sc.is_in_primary_key, sc.comment," +
            "    ic.data_type, ic.is_nullable, ic.column_default," +
            "    ic.character_maximum_length, ic.character_octet_length," +
            "    ic.numeric_precision, ic.numeric_precision_radix, ic.numeric_scale," +
            "    ic.datetime_precision " +
            "FROM system.columns AS sc INNER JOIN information_schema.columns AS ic " +
            "    ON sc.database = ic.table_schema AND sc.table = ic.table_name AND sc.name = ic.column_name " +
            "WHERE database = :database AND table = :table";

    public static final String TABLE_COLUMNS_INFO_QUERY =
            "";

    public static final String DATA_EXISTS_QUERY = "SELECT * FROM :table LIMIT 1";

    public static String createDatabaseSQL(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    public static String dropDatabaseSQL(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    public static String dropTableSQL(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP TABLE " + (ignoreIfNotExists ? "IF EXISTS " : "") + tablePath.getFullName();
    }

    public static String truncateTableSQL(TablePath tablePath) {
        return "TRUNCATE TABLE " + tablePath.getFullName();
    }
}
