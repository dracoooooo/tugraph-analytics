package com.antgroup.geaflow.dsl.connector.hive;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.utils.DateTimeUtil;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.Windows;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorConstants;
import com.antgroup.geaflow.dsl.connector.hive.utils.PulsarConstants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarTableSource.class);
    private Configuration tableConf;
    private String url;
    private String topic;
    private String subscription;
    private long windowSize;
    private int startTime;
    private PulsarClient client;
    private Consumer<String> consumer;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
        this.url = tableConf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SERVICE_URL);
        this.topic = tableConf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_TOPIC);
        this.subscription = tableConf.getString(PulsarConfigKeys.GEAFLOW_DSL_PULSAR_SUBSCRIPTION);
        this.windowSize = tableConf.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
        if (this.windowSize != Windows.SIZE_OF_ALL_WINDOW && this.windowSize < 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", windowSize);
        }
        final String startTimeStr = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_START_TIME,
            (String) ConnectorConfigKeys.GEAFLOW_DSL_START_TIME.getDefaultValue());
        if (startTimeStr.equalsIgnoreCase(PulsarConstants.PULSAR_BEGIN)) {
            startTime = 0;
        } else {
            startTime = DateTimeUtil.toUnixTime(ConnectorConstants.START_TIME_FORMAT, startTimeStr);
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            this.client = PulsarClient.builder()
                .serviceUrl(this.url)
                .build();
            BatchReceivePolicy batchReceivePolicy =
                BatchReceivePolicy.builder().timeout(PulsarConstants.PULSAR_DATA_TIMEOUT_SECONDS,
                    TimeUnit.SECONDS).build();
            ConsumerBuilder<String> consumerBuilder = client.newConsumer(Schema.STRING)
                .topic(this.topic)
                .subscriptionName(this.subscription)
                .batchReceivePolicy(batchReceivePolicy);
            if (this.windowSize == Windows.SIZE_OF_ALL_WINDOW) {
                consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
            } else if (this.windowSize > 0) {
                consumerBuilder.receiverQueueSize((int) this.windowSize);
            } else {
                throw new GeaFlowDSLException("Invalid window size: {}", windowSize);
            }
            this.consumer = consumerBuilder.subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        CompletableFuture<List<String>> cf = client.getPartitionsForTopic(this.topic);
        try {
            return cf.get().stream().map(PulsarPartition::new).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new TextDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  long newWindowSize) throws IOException {
        PulsarPartition pulsarPartition = (PulsarPartition) partition;
        if (newWindowSize != Windows.SIZE_OF_ALL_WINDOW && newWindowSize < 0) {
            throw new GeaFlowDSLException("Invalid window size: {}", newWindowSize);
        }
        this.windowSize = newWindowSize;
        this.topic = pulsarPartition.topic;

        PulsarOffset reqPulsarOffset;
        if (startOffset.isPresent()) {
            reqPulsarOffset = (PulsarOffset) startOffset.get();
            this.consumer.seek(reqPulsarOffset.getPulsarOffset());
        } else {
            if (startTime == 0) {
                this.consumer.seek(MessageId.earliest);
            } else {
                this.consumer.seek(startTime);
            }
        }

        Iterator<Message<String>> recordIterator = consumer.batchReceive().iterator();

        List<String> dataList = new ArrayList<>();
        long responseMaxTimestamp = -1;

        MessageIdImpl nextOffset = null;
        while (recordIterator.hasNext()) {
            Message<String> record = recordIterator.next();
            consumer.acknowledge(record);
            nextOffset = (MessageIdImpl) record.getMessageId();
            dataList.add(record.getValue());
            if (record.getPublishTime() > responseMaxTimestamp) {
                responseMaxTimestamp = record.getPublishTime();
            }
        }
        //reload cursor
        if (nextOffset != null) {
            nextOffset = new MessageIdImpl(nextOffset.getLedgerId(), nextOffset.getEntryId() + 1,
                nextOffset.getPartitionIndex());
        }
        if (responseMaxTimestamp >= 0) {
            reqPulsarOffset = new PulsarOffset(nextOffset, responseMaxTimestamp);
        } else {
            reqPulsarOffset = new PulsarOffset(nextOffset, System.currentTimeMillis());
        }

        return (FetchData<T>) new FetchData<>(dataList, reqPulsarOffset, false);
    }

    @Override
    public void close() {
        LOGGER.info("close");
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
            if (client != null) {
                client.close();
                client = null;
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static class PulsarPartition implements Partition {

        private final String topic;

        public PulsarPartition(String topic) {
            this.topic = topic;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PulsarPartition)) {
                return false;
            }
            PulsarPartition that = (PulsarPartition) o;
            return Objects.equals(topic, that.topic);
        }

        @Override
        public String getName() {
            return topic;
        }
    }

    public static class PulsarOffset implements Offset {

        private final MessageId offset;

        private final long humanReadableTime;

        public PulsarOffset(MessageId offset, long humanReadableTime) {
            this.offset = offset;
            this.humanReadableTime = humanReadableTime;
        }

        @Override
        public String humanReadable() {
            return DateTimeUtil.fromUnixTime(humanReadableTime,
                ConnectorConstants.START_TIME_FORMAT);
        }

        @Override
        public long getOffset() {
            return humanReadableTime;
        }

        public MessageId getPulsarOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return true;
        }
    }
}
