package com.antgroup.geaflow.dsl.connector.hive;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class PulsarConfigKeys {
    public static final ConfigKey GEAFLOW_DSL_PULSAR_SERVICE_URL = ConfigKeys
        .key("geaflow.dsl.pulsar.serviceurl")
        .noDefaultValue()
        .description("The service URL for Pulsar.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_TOPIC = ConfigKeys
        .key("geaflow.dsl.pulsar.topic")
        .noDefaultValue()
        .description("The pulsar topic name.");

    // todo: subscription mode
    public static final ConfigKey GEAFLOW_DSL_PULSAR_SUBSCRIPTION = ConfigKeys
        .key("geaflow.dsl.pulsar.subscription")
        .noDefaultValue()
        .description("The pulsar subscription name.");
}
