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

package com.antgroup.geaflow.dsl.connector.file.sink;

import static com.antgroup.geaflow.dsl.connector.file.FileConstants.PREFIX_LOCAL_FILE;

public class FileWriteHandlers {

    public static FileWriteHandler from(String path) {
        if (path.startsWith(PREFIX_LOCAL_FILE)) {
            return new LocalFileWriteHandler(path);
        }
        return new HdfsFileWriteHandler(path);
    }
}
