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
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;

// reference https://clickhouse.com/docs/en/sql-reference/data-types
@Slf4j
@AutoService(TypeConverter.class)
public class ClickhouseTypeConverter implements TypeConverter<BasicTypeDefine<ClickHouseDataType>> {

    public static final ClickhouseTypeConverter INSTANCE = new ClickhouseTypeConverter();

    @Override
    public String identifier() {
        return "Clickhouse";
    }

    @Override
    public Column convert(BasicTypeDefine<ClickHouseDataType> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        return builder.build();
    }

    @Override
    public BasicTypeDefine<ClickHouseDataType> reconvert(Column column) {
        return null;
    }
}
