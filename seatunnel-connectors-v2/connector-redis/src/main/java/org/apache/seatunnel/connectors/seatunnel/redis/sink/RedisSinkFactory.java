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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class RedisSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Redis";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new RedisSink(context.getOptions(), catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        RedisConfig.HOST, RedisConfig.PORT, RedisConfig.KEY, RedisConfig.DATA_TYPE)
                .optional(
                        RedisConfig.MODE,
                        RedisConfig.AUTH,
                        RedisConfig.USER,
                        RedisConfig.KEY_PATTERN,
                        RedisConfig.FORMAT,
                        RedisConfig.EXPIRE,
                        RedisConfig.SUPPORT_CUSTOM_KEY,
                        RedisConfig.VALUE_FIELD,
                        RedisConfig.HASH_KEY_FIELD,
                        RedisConfig.HASH_VALUE_FIELD,
                        SinkCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(RedisConfig.MODE, RedisConfig.RedisMode.CLUSTER, RedisConfig.NODES)
                .build();
    }
}
