/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.contribs.http;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.netflix.conductor.contribs.http.HttpTask.Input;
import com.netflix.conductor.core.config.Configuration;
import com.sun.jersey.api.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Viren
 * Provider for Jersey Client.  This class provides an 
 */
@Singleton
public class RestClientManager {
    private static final Logger logger = LoggerFactory.getLogger(RestClientManager.class);

    static final int DEFAULT_READ_TIMEOUT = 150;
    static final int DEFAULT_CONNECT_TIMEOUT = 100;
    static final String HTTP_TASK_READ_TIMEOUT = "http.task.read.timeout";
    static final String HTTP_TASK_CONNECT_TIMEOUT = "http.task.connect.timeout";


    private final ThreadLocal<Client> threadLocalClient;
    private final int defaultReadTimeout;
    private final int defaultConnectTimeout;

    private NamingService naming;
    private final Pattern LB_PATTERN = Pattern.compile("((lb)|(LB))://([a-zA-Z0-9_-]+)(/?.*)");

    public RestClientManager(Configuration config) {
        this.threadLocalClient = ThreadLocal.withInitial(() -> Client.create());
        this.defaultReadTimeout = config.getIntProperty(HTTP_TASK_READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        this.defaultConnectTimeout = config.getIntProperty(HTTP_TASK_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);

        String registryServer = System.getProperty("workflow.service.registry");
        if (!Objects.isNull(registryServer)) {
            try {
                naming = NamingFactory.createNamingService(registryServer);
            } catch (NacosException e) {
                logger.error("create naming service error", e);
            }
        }
    }

    public Client getClient(Input input) {
        Client client = threadLocalClient.get();
        client.setReadTimeout(defaultReadTimeout);
        client.setConnectTimeout(defaultConnectTimeout);
        return client;
    }

    // 使用负载均衡策略替换服务名为具体地址
    public String convertUri(String uri) {
        if (Objects.isNull(naming)) {
            logger.error("Naming service is null ,can't parse url:{}", uri);
            return uri;
        }
        UriConverter uriObj = parseUri(uri);
        if (Objects.isNull(uriObj)) {
            return uri;
        }
        Instance instance = null;
        try {
            instance = naming.selectOneHealthyInstance(uriObj.serviceId);
        } catch (NacosException e) {
            logger.error("Get serviceId:{} from naming service error!", uriObj.serviceId);
            logger.error("Error message:", e);
            return uri;
        }
        return uriObj.initUri(instance);
    }

    private UriConverter parseUri(String uri) {
        Matcher m = LB_PATTERN.matcher(uri);
        if (m.matches()) {
            String schema = m.group(1);
            String serviceId = m.group(4);
            String path = m.group(5);
            return new UriConverter(schema, serviceId, path);
        } else {
            logger.error("Not found serviceId from uri:{}", uri);
            return null;
        }
    }

    static class UriConverter {
        String schema;
        String serviceId;
        String path;

        public UriConverter(String schema, String serviceId, String path) {
            this.schema = schema;
            this.serviceId = serviceId;
            this.path = path;
        }

        public String initUri(Instance instance) {
            return String.format("%s://%s:%d%s", "http", instance.getIp(), instance.getPort(), path);
        }
    }
}
