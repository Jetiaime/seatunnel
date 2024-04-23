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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog;


import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;


import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseCatalogUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ClickhouseCatalog implements Catalog {

    private static final Set<String> SYS_DATABASES = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseCatalog.class);

    static {
        SYS_DATABASES.add("system");
        SYS_DATABASES.add("information_schema");
    }

    protected final String catalogName;

    protected final String host;

    protected final String database;

    protected final String serverTimeZone;

    protected final String username;

    protected final String password;

    protected final ClickhouseProxy clickhouseProxy;

    protected String defaultDatabase = "default";

    public ClickhouseCatalog(String catalogName, String host, String database, String serverTimeZone, String username, String password) {
        this.catalogName = catalogName;
        this.host = host;
        this.database = database;
        this.serverTimeZone = serverTimeZone;
        this.username = username;
        this.password = password;

        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(
                this.host,
                this.database,
                this.serverTimeZone,
                this.username,
                this.password
        );
        this.clickhouseProxy = new ClickhouseProxy(nodes.get(0));
    }

    public ClickhouseCatalog(String catalogName, String host, String database, String serverTimeZone, String username, String password, String defaultDatabase) {
        this(catalogName, host, database, serverTimeZone, username, password);
        this.defaultDatabase = defaultDatabase;
    }

    private Map<String, String> connectionOptions() {
        // TODO: add more options
        Map<String, String> options = new HashMap<>();
        options.put("connector", "clickhouse");
        options.put("host", host);
        options.put("database", database);
        return options;
    }

    @Override
    public void open() throws CatalogException {
        String testSQL = "SELECT 1";

        try {
            ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
            connection.query(testSQL).executeAndWait();
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to connect host %s", host), e);
        }
        LOG.info("Catalog {} established connection to {} success", catalogName, host);
    }

    @Override
    public void close() throws CatalogException {
        clickhouseProxy.close();
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.DATABASES_QUERY_WITH_IDENTIFIER_QUERY)
                .params(Collections.singletonMap("database", databaseName))
                .executeAndWait()) {
            return response.records().iterator().hasNext();
        } catch (ClickHouseException e) {
            throw new CatalogException("check database exists failed", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.ALL_DATABASE_QUERY)
                .executeAndWait()) {
            return response
                    .stream()
                    .map(r -> r.getValue(0).asString())
                    .filter(name -> !SYS_DATABASES.contains(name))
                    .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new CatalogException("list databases failed", e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try {
            connection.query(ClickhouseCatalogUtil.createDatabaseSQL(tablePath.getDatabaseName(), ignoreIfExists))
                    .executeAndWait();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("create database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try {
            connection.query(ClickhouseCatalogUtil.dropDatabaseSQL(tablePath.getDatabaseName(), ignoreIfNotExists))
                    .executeAndWait();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("drop database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.TABLES_QUERY_WITH_IDENTIFIER_QUERY)
                .params(Collections.singletonMap("database", tablePath.getDatabaseName()))
                .params(Collections.singletonMap("name", tablePath.getTableName()))
                .executeAndWait()) {
            return response.records().iterator().hasNext();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("check table [%s] exists failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException, DatabaseNotExistException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.TABLES_QUERY_WITH_DATABASE_QUERY)
                .params(Collections.singletonMap("database", databaseName))
                .executeAndWait()) {
            return response
                    .stream()
                    .map(r -> r.getValue(0).asString())
                    .collect(Collectors.toList());
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("list tables of database [%s] failed", databaseName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();

        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.TABLE_INFO_QUERY)
                .params(Collections.singletonMap("database", tablePath.getDatabaseName()))
                .params(Collections.singletonMap("table", tablePath.getTableName()))
                .executeAndWait()) {

            List<String> partitionKeys = new ArrayList<>();
            List<String> primaryKeys = new ArrayList<>();
            TableSchema.Builder builder = TableSchema.builder();

            response.stream().forEach(r -> {
                String name = r.getValue("name").asString();
                boolean isInPartitionKey = r.getValue("is_in_partition_key").asBoolean();
                boolean isInPrimaryKey = r.getValue("is_in_primary_key").asBoolean();
                String comment = r.getValue("comment").asString();
                String originalType = r.getValue("data_type").asString();
                boolean isNullable = r.getValue("is_nullable").asBoolean();
                String columnDefault = r.getValue("column_default").asString();
                int characterMaximumLength = r.getValue("character_maximum_length").asInteger();
                int characterOctetLength = r.getValue("character_octet_length").asInteger();
                int numericPrecision = r.getValue("numeric_precision").asInteger();
                int numericPrecisionRadix = r.getValue("numeric_precision_radix").asInteger();
                int numericScale = r.getValue("numeric_scale").asInteger();
                int datetimePrecision = r.getValue("datetime_precision").asInteger();

                if (isInPartitionKey) {
                    partitionKeys.add(name);
                }
                if (isInPrimaryKey) {
                    primaryKeys.add(name);
                }

                String columnType = originalType;
                String dataType = originalType;
                if (isNullable) {
                    columnType = dataType.substring(dataType.indexOf("Nullable(") + 9, dataType.lastIndexOf(")"));
                }
                dataType = columnType.split("\\(")[0];
                ClickHouseDataType chType = ClickHouseDataType.valueOf(dataType);
                Long precision = null;
                Long length = null;
                switch (chType) {
                    case FixedString:
                        case String:
                            length = Long.parseLong(
                                    columnType.substring(
                                            columnType.indexOf("(") + 1,
                                            columnType.indexOf(")"))
                            );
                        break;
                    case Decimal:
                        precision = (long) numericPrecision;
                        break;
                    case DateTime64:
                        precision = (long) datetimePrecision;
                        break;
                }

                BasicTypeDefine<ClickHouseDataType> typeDefine =
                        BasicTypeDefine.<ClickHouseDataType>builder()
                                .name(name)
                                .columnType(columnType)
                                .dataType(dataType)
                                .nativeType(chType)
                                .nullable(isNullable)
                                .defaultValue(columnDefault)
                                .unsigned(chType.isSigned())
                                .length(length)
                                .precision(precision)
                                .scale(numericScale)
                                .comment(comment)
                                .build();

                builder.column(ClickhouseTypeConverter.INSTANCE.convert(typeDefine));
            });

            if (!primaryKeys.isEmpty()) {
                builder.primaryKey(
                        PrimaryKey.of(
                                "pk_"
                                        + tablePath.getDatabaseName()
                                        + "_"
                                        + tablePath.getTableName(),
                                partitionKeys));
            }

            return CatalogTable.of(
                    TableIdentifier.of(
                            catalogName,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName()),
                    builder.build(),
                    connectionOptions(),
                    partitionKeys,
                    StringUtils.EMPTY,
                    catalogName
            );
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("get table [%s] failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public <T> void buildColumnsWithErrorCheck(TablePath tablePath, TableSchema.Builder builder, Iterator<T> keys, Function<T, Column> getColumn) {
        Catalog.super.buildColumnsWithErrorCheck(tablePath, builder, keys, getColumn);
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        // TODO: implement createTable
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try {
            connection.query(ClickhouseCatalogUtil.dropTableSQL(tablePath, ignoreIfNotExists)).executeAndWait();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("drop table [%s] failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try {
            connection.query(ClickhouseCatalogUtil.truncateTableSQL(tablePath)).executeAndWait();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("truncate table [%s] failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        ClickHouseRequest<?> connection = clickhouseProxy.getClickhouseConnection();
        try (ClickHouseResponse response = connection
                .query(ClickhouseCatalogUtil.DATA_EXISTS_QUERY)
                .params(Collections.singletonMap("table", tablePath.getFullName()))
                .executeAndWait()) {
            return response.records().iterator().hasNext();
        } catch (ClickHouseException e) {
            throw new CatalogException(
                    String.format("check table [%s] data exists failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public PreviewResult previewAction(ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        // TODO: implement previewAction
        return Catalog.super.previewAction(actionType, tablePath, catalogTable);
    }
}
