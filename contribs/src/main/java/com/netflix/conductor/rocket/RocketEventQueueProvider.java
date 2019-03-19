package com.netflix.conductor.rocket;

import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author litian
 */
public class RocketEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(RocketEventQueueProvider.class);
    private final Map<String, ObservableQueue> queues = new ConcurrentHashMap<>();

    private Configuration config;

    @Inject
    public RocketEventQueueProvider(Configuration config) {
        this.config = config;
        logger.info("Rocket Event Queue Provider initialized.");
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new RocketObservableQueue(queueURI, config));

    }
}
