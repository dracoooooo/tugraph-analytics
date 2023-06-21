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

package com.antgroup.geaflow.dsl.runtime.query;

import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.testenv.SourceFunctionNoPartitionCheck;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class PulsarTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTest.class);

    @Test
    public void testPulsar_001() throws Exception {
        Map<String, String> config  = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(1L));
        config.put(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION.getKey(),
            SourceFunctionNoPartitionCheck.class.getName());
        QueryTester
            .build()
            .withQueryPath("/query/pulsar_write_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute();
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/pulsar_scan_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60);
        try {
            tester.execute();
        } catch (GeaFlowDSLException e) {
            LOGGER.info("Kafka unbounded stream finish with timeout.");
        }
        tester.checkSinkResult();
    }

}
