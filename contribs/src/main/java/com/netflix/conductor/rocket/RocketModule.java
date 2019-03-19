package com.netflix.conductor.rocket;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Named;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author litian
 */
public class RocketModule extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(RocketModule.class);

    @Override
    protected void configure() {
        logger.info("rocket module configured.");
    }

    @ProvidesIntoMap
    @StringMapKey("rocket")
    @Singleton
    @Named("EventQueueProviders")
    public EventQueueProvider getRocketEventQueueProvider(Configuration configuration) {
        RocketEventQueueProvider provider = new RocketEventQueueProvider(configuration);
        // 初始化队列提供者的同时，初始化Wait Task的队列
        initWaitQueue(configuration, provider);
        return provider;
    }

    // 这里封装的逻辑是对系统Wait Task的支持
    // 默认有两个队列用于监控接收完成和失败的任务，分别为：
    // listener_queue_COMPLETED和listener_queue_FAILED
    // 可以使用配置项workflow.listener.queue.prefix来配置队列名前缀
    @Provides
    public Map<Task.Status, ObservableQueue> getQueues(Configuration config, RocketEventQueueProvider rocket) {
        return initWaitQueue(config, rocket);
    }

    private Map<Task.Status, ObservableQueue> initWaitQueue(Configuration config, RocketEventQueueProvider rocket) {
        Task.Status[] statuses = new Task.Status[]{Task.Status.COMPLETED, Task.Status.FAILED};
        Map<Task.Status, ObservableQueue> queues = new HashMap<>();
        for (Task.Status status : statuses) {
            String queueName = config.getProperty("workflow.listener.queue.prefix", "listener_queue");
            queueName += "_" + status.name();
            logger.info("add workflow status listener queue: {}", queueName);

            ObservableQueue queue = rocket.getQueue(queueName);
            if (Objects.isNull(queue)) {
                logger.error("get queue error! queueName:{}", queueName);
                continue;
            }
            queues.put(status, queue);
        }
        return queues;
    }
}
