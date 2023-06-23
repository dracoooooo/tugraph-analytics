/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.dsl.connector.jdbc;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class JDBCConfigKeys {
    public static final ConfigKey GEAFLOW_DSL_JDBC_DRIVER = ConfigKeys
        .key("geaflow.dsl.jdbc.driver")
        .noDefaultValue()
        .description("The JDBC driver.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_URL = ConfigKeys
        .key("geaflow.dsl.jdbc.url")
        .noDefaultValue()
        .description("The database URL.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_USERNAME = ConfigKeys
        .key("geaflow.dsl.jdbc.username")
        .noDefaultValue()
        .description("The database username.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_PASSWORD = ConfigKeys
        .key("geaflow.dsl.jdbc.password")
        .noDefaultValue()
        .description("The database password.");

    public static final ConfigKey GEAFLOW_DSL_JDBC_TABLE_NAME = ConfigKeys
        .key("geaflow.dsl.jdbc.table.name")
        .noDefaultValue()
        .description("The table name.");
}
