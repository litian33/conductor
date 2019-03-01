package com.netflix.conductor.rocket;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author litian
 */
public class RocketObservableQueue implements ObservableQueue {
    private static Logger logger = LoggerFactory.getLogger(RocketObservableQueue.class);

    private static final String QUEUE_TYPE = "rocket";

    private static final String ROCKET_PRODUCER_PREFIX = "rocket.producer.";

    private static final String ROCKET_CONSUMER_PREFIX = "rocket.consumer.";

    private static final String ROCKET_PREFIX = "rocket.";

    private String queueName;

    private int sendBatchSize;
    private int pollBatchSize;

    private final int pollIntervalInMS;

    private final int pollTimeoutInMs;
    private final int sendTimeoutInMs;


    private DefaultMQProducer producer;

    private DefaultMQPullConsumer consumer;

    private Map<MessageQueue, Long> offsetTable = new HashMap<>();


    @Inject
    public RocketObservableQueue(String queueName, Configuration config) {
        this.queueName = queueName;

        this.pollIntervalInMS = config.getIntProperty("rocket.consumer.pollingInterval", 1000);
        this.pollTimeoutInMs = config.getIntProperty("rocket.consumer.longPollTimeout", 1000);
        this.sendBatchSize = config.getIntProperty("rocket.producer.batchSize", 32);
        this.pollBatchSize = config.getIntProperty("rocket.consumer.batchSize", 32);
        this.sendTimeoutInMs = config.getIntProperty("rocket.producer.sendTimeout", 1000);
        init(config);
    }

    private void init(Configuration config) {
        Properties producerProps = new Properties();
        Properties consumerProps = new Properties();
        consumerProps.put("group.id", queueName + "_group");
        String serverId = config.getServerId();
        consumerProps.put("consumer.id", queueName + "_consumer_" + serverId);
        producerProps.put("producer.id", queueName + "_producer_" + serverId);

        Map<String, Object> configMap = config.getAll();
        if (Objects.isNull(configMap)) {
            throw new RuntimeException("Configuration missing");
        }
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            String key = entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(ROCKET_PREFIX)) {
                if (key.startsWith(ROCKET_PRODUCER_PREFIX)) {
                    producerProps.put(key.replaceAll(ROCKET_PRODUCER_PREFIX, ""), value);
                } else if (key.startsWith(ROCKET_CONSUMER_PREFIX)) {
                    consumerProps.put(key.replaceAll(ROCKET_CONSUMER_PREFIX, ""), value);
                } else {
                    producerProps.put(key.replaceAll(ROCKET_PREFIX, ""), value);
                    consumerProps.put(key.replaceAll(ROCKET_PREFIX, ""), value);
                }
            }
        }

        try {
            // Init rocket producer and consumer
            String producerGroup = producerProps.getProperty("producer.id");
            String consumerGroup = consumerProps.getProperty("consumer.id");
            String mqServer = consumerProps.getProperty("servers");

            producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(mqServer);
            producer.start();

            consumer = new DefaultMQPullConsumer(consumerGroup);
            consumer.setNamesrvAddr(mqServer);
            consumer.start();

            logger.info("RocketObservableQueue initialized for {}", queueName);
        } catch (MQClientException ke) {
            logger.error("Rocket initialization failed.", ke);
            throw new RuntimeException(ke);
        }
    }


    @Override
    public Observable<Message> observe() {
        Observable.OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    @VisibleForTesting
    Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(this.pollIntervalInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x) -> {
                List<Message> messages = receiveMessages();
                return Observable.from(messages);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    private void putMessageQueueOffset(MessageQueue mq, long offset) {
        offsetTable.put(mq, offset);
    }

    private long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offsetTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

    @VisibleForTesting()
    List<Message> receiveMessages() {
        List<Message> messages = new ArrayList<>();
        Set<MessageQueue> mqs = null;
        try {
            mqs = consumer.fetchSubscribeMessageQueues(this.queueName);
        } catch (MQClientException e) {
            // 如果刚开始主题没有创建，这里一直获取不成功，日志打印太频繁，所以屏蔽此部分日志
            //logger.error("rocket consumer message polling failed.", e);
            return messages;
        }
        if (Objects.isNull(mqs)) {
            logger.error("get  rocket message queue error! queueName:{}", this.queueName);
            return messages;
        }
        mqs.forEach(mq -> {
            PullResult result;
            try {
                result = consumer.pull(mq, null, getMessageQueueOffset(mq), this.pollBatchSize, this.pollTimeoutInMs);
            } catch (Exception e) {
                logger.error("rocket consumer message polling failed.", e);
                return;
            }

            putMessageQueueOffset(mq, result.getNextBeginOffset());

            switch (result.getPullStatus()) {
                case FOUND:
                    List<MessageExt> messageExtList = result.getMsgFoundList();
                    for (MessageExt m : messageExtList) {
                        Message msg = new Message(m.getMsgId(), new String(m.getBody(), Charset.forName("UTF-8")), "");
                        messages.add(msg);
                    }
                    break;
                default:
                    break;
            }

        });
        return messages;
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return this.queueName;
    }

    @Override
    public String getURI() {
        return this.queueName;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        // TODO rocketmq没有手动确认消息的过程，先留空，假设全部确认成功
        return Collections.emptyList();
    }

    @Override
    public void publish(List<Message> messages) {
        logger.info("Sending {} messages to the SQS queue: {}", messages.size(), queueName);
        List<org.apache.rocketmq.common.message.Message> batch = new ArrayList<>();
        messages.forEach(msg -> {
            org.apache.rocketmq.common.message.Message message = new org.apache.rocketmq.common.message.Message(this.queueName, "", msg.getId(), msg.getPayload().getBytes(Charset.forName("UTF-8")));
            message.putUserProperty("receipt", msg.getReceipt());
            batch.add(message);
            if (batch.size() >= this.sendBatchSize) {
                sendMsg(batch);
                batch.clear();
            }
        });
        sendMsg(batch);
    }

    private void sendMsg(List<org.apache.rocketmq.common.message.Message> batch) {
        if (batch.isEmpty()) {
            return;
        }
        logger.info("sending {} messages in batch", batch.size());
        try {
            SendResult result = producer.send(batch, this.sendTimeoutInMs);
            SendStatus status = result.getSendStatus();
            logger.info("send result: {} for rocket queue: {}", status.name(), queueName);
        } catch (Exception e) {
            logger.error("send result: {} for rocket queue: {}", e.getMessage(), queueName);
            logger.error("", e);
        }
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
        logger.info("set-Un-ack-Timeout for message:{}, and timeout:{}", message, unackTimeout);
    }

    @Override
    public long size() {
        logger.info("get queue size is 0");
        return 0;
    }

    @Override
    public void close() {
        if (this.consumer != null) {
            this.consumer.shutdown();
        }

        if (this.producer != null) {
            this.producer.shutdown();
        }
    }
}
