package com.antgroup.geaflow.dsl.connector.hive;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTableSink.class);

    private Configuration tableConf;
    private StructType schema;
    private String url;
    private String topic;
    private String separator;
    private PulsarClient client;
    private Producer<String> producer;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("prepare with config: {}, \n schema: {}", tableConf, schema);

        this.tableConf = tableConf;
        this.schema = schema;
        this.url = tableConf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVICE_URL);
        this.topic = tableConf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC);
        this.separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            this.client = PulsarClient.builder()
                .serviceUrl(this.url)
                .build();
            this.producer = client.newProducer(Schema.STRING)
                .topic(this.topic)
                .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        Object[] values = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            values[i] = row.getField(i, schema.getType(i));
        }
        String msg = StringUtils.join(values, separator);
        producer.send(msg);
    }

    private void flush() throws PulsarClientException {
        LOGGER.info("flush");
        if (producer != null) {
            producer.flush();
        } else {
            LOGGER.warn("Producer is null.");
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        LOGGER.info("close");
        try {
            flush();
            if (producer != null) {
                producer.close();
            }
            if (client != null) {
                client.close();
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
