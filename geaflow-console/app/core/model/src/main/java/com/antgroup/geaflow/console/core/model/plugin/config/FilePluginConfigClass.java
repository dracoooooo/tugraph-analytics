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

package com.antgroup.geaflow.console.core.model.plugin.config;

import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FilePluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.file.path", comment = "文件路径")
    @GeaflowConfigValue(required = true, defaultValue = "/")
    private String filePath;

    @GeaflowConfigKey(value = "geaflow.dsl.column.separator", comment = "列分隔符")
    @GeaflowConfigValue(defaultValue = ",")
    private String columnSeparator;

    @GeaflowConfigKey(value = "geaflow.dsl.line.separator", comment = "行分隔符")
    @GeaflowConfigValue(defaultValue = "\\n")
    private String lineSeparator;

    @GeaflowConfigKey(value = "geaflow.file.persistent.config.json", comment = "DFS配置")
    @GeaflowConfigValue(defaultValue = "{\"fs.defaultFS\":\"local\"}")
    private String persistConfig;

    public FilePluginConfigClass() {
        super(GeaflowPluginType.FILE);
    }
}
