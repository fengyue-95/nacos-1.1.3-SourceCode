/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.client.config;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigRequest;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.http.MetricsHttpAgent;
import com.alibaba.nacos.client.config.http.ServerHttpAgent;
import com.alibaba.nacos.client.config.impl.ClientWorker;
import com.alibaba.nacos.client.config.impl.HttpSimpleClient.HttpResult;
import com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.ParamUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.StringUtils;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.client.utils.TenantUtil;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Config Impl
 *
 * @author Nacos
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosConfigService implements ConfigService {

    private static final Logger LOGGER = LogUtils.logger(NacosConfigService.class);

    private static final long POST_TIMEOUT = 3000L;

    private static final String EMPTY = "";

    /**
     * http agent
     */
    private HttpAgent agent;
    /**
     * longpolling
     */
    private ClientWorker worker;
    private String namespace;
    private String encode;
    private ConfigFilterChainManager configFilterChainManager = new ConfigFilterChainManager();

    public NacosConfigService(Properties properties) throws NacosException {
        //给配置文件设置编码格式，默认为utf-8
        String encodeTmp = properties.getProperty(PropertyKeyConst.ENCODE);
        if (StringUtils.isBlank(encodeTmp)) {
            encode = Constants.ENCODE;
        } else {
            encode = encodeTmp.trim();
        }
        //初始化命名空间
        initNamespace(properties);
        //实例化了MetricsHttpAgent类，该类持有ServerHttpAgent类的实例，
        //ServerHttpAgent主要实现与Nacos服务器之间的通信，包括配置发布、删除等等
        //这里的MetricsHttpAgent是对ServerHttpAgent的一层包装
        //MetricsHttpAgent/ServerHttpAgent都实现了httpagent接口
        //MetricsHttpAgent实现了httpget，httppost，httpdelete，实现了对监控指标的操作。
        agent = new MetricsHttpAgent(new ServerHttpAgent(properties));

        //start方法适用于获取Nacos服务器列表的
        agent.start();
        //实现longpolling的配置更新任务，如果客户端订阅了某个配置，就会有ClientWorker来获取最新的配置更新，并通知响应的监听类。
        //完成构造函数的分析
        worker = new ClientWorker(agent, configFilterChainManager, properties);
    }

    /**
     * 初始化命名空间
     * @param properties
     */
    private void initNamespace(Properties properties) {
        String namespaceTmp = null;

        String isUseCloudNamespaceParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                System.getProperty(SystemPropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                    String.valueOf(Constants.DEFAULT_USE_CLOUD_NAMESPACE_PARSING)));

        if (Boolean.valueOf(isUseCloudNamespaceParsing)) {
            namespaceTmp = TemplateUtils.stringBlankAndThenExecute(namespaceTmp, new Callable<String>() {
                @Override
                public String call() {
                    return TenantUtil.getUserTenantForAcm();
                }
            });

            namespaceTmp = TemplateUtils.stringBlankAndThenExecute(namespaceTmp, new Callable<String>() {
                @Override
                public String call() {
                    String namespace = System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_NAMESPACE);
                    return StringUtils.isNotBlank(namespace) ? namespace : EMPTY;
                }
            });
        }
        //获取配置文件的命名空间
        if (StringUtils.isBlank(namespaceTmp)) {
            namespaceTmp = properties.getProperty(PropertyKeyConst.NAMESPACE);
        }
        //如果配置文件也没有指定namespace，默认为""
        namespace = StringUtils.isNotBlank(namespaceTmp) ? namespaceTmp.trim() : EMPTY;
        properties.put(PropertyKeyConst.NAMESPACE, namespace);
    }

    /**
     *  获取配置
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @return
     * @throws NacosException
     */
    @Override
    public String getConfig(String dataId, String group, long timeoutMs) throws NacosException {
        return getConfigInner(namespace, dataId, group, timeoutMs);
    }

    /**
     * 获取配置并注册监听器
     * @param dataId    dataId
     * @param group     group
     * @param timeoutMs read timeout
     * @param listener {@link Listener}
     * @return
     * @throws NacosException
     */
    @Override
    public String getConfigAndSignListener(String dataId, String group, long timeoutMs, Listener listener) throws NacosException {
        String content = getConfig(dataId, group, timeoutMs);
        worker.addTenantListenersWithContent(dataId, group, content, Arrays.asList(listener));
        return content;
    }

    /**
     * 向Worker注册监听器
     * @param dataId   dataId
     * @param group    group
     * @param listener listener
     * @throws NacosException
     */
    @Override
    public void addListener(String dataId, String group, Listener listener) throws NacosException {
        worker.addTenantListeners(dataId, group, Arrays.asList(listener));
    }

    /**
     * 发布配置
     * @param dataId  dataId
     * @param group   group
     * @param content content
     * @return
     * @throws NacosException
     */
    @Override
    public boolean publishConfig(String dataId, String group, String content) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content);
    }

    /**
     * 移除配置
     * @param dataId dataId
     * @param group  group
     * @return
     * @throws NacosException
     */
    @Override
    public boolean removeConfig(String dataId, String group) throws NacosException {
        return removeConfigInner(namespace, dataId, group, null);
    }

    /**
     * 移除worker的监听器
     * @param dataId   dataId
     * @param group    group
     * @param listener listener
     */
    @Override
    public void removeListener(String dataId, String group, Listener listener) {
        worker.removeTenantListener(dataId, group, listener);
    }

    /**
     * 获取配置
     * @param tenant
     * @param dataId
     * @param group
     * @param timeoutMs
     * @return
     * @throws NacosException
     */
    private String getConfigInner(String tenant, String dataId, String group, long timeoutMs) throws NacosException {
        group = null2defaultGroup(group);
        ParamUtils.checkKeyParam(dataId, group);
        ConfigResponse cr = new ConfigResponse();

        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);

        // 1.优先使用本地配置
        String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get failover ok, dataId={}, group={}, tenant={}, config={}", agent.getName(),
                dataId, group, tenant, ContentUtils.truncateContent(content));
            cr.setContent(content);
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();
            return content;
        }

        try {
            //2.如果本地配置不存在，其次使用worker请求，去server去获取配置信息
            content = worker.getServerConfig(dataId, group, tenant, timeoutMs);

            cr.setContent(content);

            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();

            return content;
        } catch (NacosException ioe) {
            if (NacosException.NO_RIGHT == ioe.getErrCode()) {
                throw ioe;
            }
            LOGGER.warn("[{}] [get-config] get from server error, dataId={}, group={}, tenant={}, msg={}",
                agent.getName(), dataId, group, tenant, ioe.toString());
        }

        LOGGER.warn("[{}] [get-config] get snapshot ok, dataId={}, group={}, tenant={}, config={}", agent.getName(),
            dataId, group, tenant, ContentUtils.truncateContent(content));

        //3.如果configserver.error,则去获取本地缓存文件内容
        //本地缓存什么时候设置的？com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor.saveSnapshot
        //com.alibaba.nacos.client.config.impl.ClientWorker.getServerConfig在调用getserviceconfig()方法的时候会将配置信息缓存一份。
        content = LocalConfigInfoProcessor.getSnapshot(agent.getName(), dataId, group, tenant);
        cr.setContent(content);
        configFilterChainManager.doFilter(null, cr);
        content = cr.getContent();
        return content;
    }

    /**
     * 如果没传group参数，默认使用DEFAULT_GROUP
     * @param group
     * @return
     */
    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * 移除配置服务，使用的是com.alibaba.nacos.client.config.http.MetricsHttpAgent#httpDelete(java.lang.String, java.util.List, java.util.List, java.lang.String, long)
     * @param tenant
     * @param dataId
     * @param group
     * @param tag
     * @return
     * @throws NacosException
     */
    private boolean removeConfigInner(String tenant, String dataId, String group, String tag) throws NacosException {
        group = null2defaultGroup(group);
        ParamUtils.checkKeyParam(dataId, group);
        String url = Constants.CONFIG_CONTROLLER_PATH;
        List<String> params = new ArrayList<String>();
        params.add("dataId");
        params.add(dataId);
        params.add("group");
        params.add(group);
        if (StringUtils.isNotEmpty(tenant)) {
            params.add("tenant");
            params.add(tenant);
        }
        if (StringUtils.isNotEmpty(tag)) {
            params.add("tag");
            params.add(tag);
        }
        HttpResult result = null;
        try {
            result = agent.httpDelete(url, null, params, encode, POST_TIMEOUT);
        } catch (IOException ioe) {
            LOGGER.warn("[remove] error, " + dataId + ", " + group + ", " + tenant + ", msg: " + ioe.toString());
            return false;
        }

        if (HttpURLConnection.HTTP_OK == result.code) {
            LOGGER.info("[{}] [remove] ok, dataId={}, group={}, tenant={}", agent.getName(), dataId, group, tenant);
            return true;
        } else if (HttpURLConnection.HTTP_FORBIDDEN == result.code) {
            LOGGER.warn("[{}] [remove] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(), dataId,
                group, tenant, result.code, result.content);
            throw new NacosException(result.code, result.content);
        } else {
            LOGGER.warn("[{}] [remove] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(), dataId,
                group, tenant, result.code, result.content);
            return false;
        }
    }

    /**
     * 发布配置，使用com.alibaba.nacos.client.config.http.MetricsHttpAgent#httpPost(java.lang.String, java.util.List, java.util.List, java.lang.String, long)
     * @param tenant
     * @param dataId
     * @param group
     * @param tag
     * @param appName
     * @param betaIps
     * @param content
     * @return
     * @throws NacosException
     */
    private boolean publishConfigInner(String tenant, String dataId, String group, String tag, String appName,
                                       String betaIps, String content) throws NacosException {
        group = null2defaultGroup(group);
        ParamUtils.checkParam(dataId, group, content);

        ConfigRequest cr = new ConfigRequest();
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);
        cr.setContent(content);
        configFilterChainManager.doFilter(cr, null);
        content = cr.getContent();

        String url = Constants.CONFIG_CONTROLLER_PATH;
        List<String> params = new ArrayList<String>();
        params.add("dataId");
        params.add(dataId);
        params.add("group");
        params.add(group);
        params.add("content");
        params.add(content);
        if (StringUtils.isNotEmpty(tenant)) {
            params.add("tenant");
            params.add(tenant);
        }
        if (StringUtils.isNotEmpty(appName)) {
            params.add("appName");
            params.add(appName);
        }
        if (StringUtils.isNotEmpty(tag)) {
            params.add("tag");
            params.add(tag);
        }

        List<String> headers = new ArrayList<String>();
        if (StringUtils.isNotEmpty(betaIps)) {
            headers.add("betaIps");
            headers.add(betaIps);
        }

        HttpResult result = null;
        try {
            result = agent.httpPost(url, headers, params, encode, POST_TIMEOUT);
        } catch (IOException ioe) {
            LOGGER.warn("[{}] [publish-single] exception, dataId={}, group={}, msg={}", agent.getName(), dataId,
                group, ioe.toString());
            return false;
        }

        if (HttpURLConnection.HTTP_OK == result.code) {
            LOGGER.info("[{}] [publish-single] ok, dataId={}, group={}, tenant={}, config={}", agent.getName(), dataId,
                group, tenant, ContentUtils.truncateContent(content));
            return true;
        } else if (HttpURLConnection.HTTP_FORBIDDEN == result.code) {
            LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(),
                dataId, group, tenant, result.code, result.content);
            throw new NacosException(result.code, result.content);
        } else {
            LOGGER.warn("[{}] [publish-single] error, dataId={}, group={}, tenant={}, code={}, msg={}", agent.getName(),
                dataId, group, tenant, result.code, result.content);
            return false;
        }

    }

    @Override
    public String getServerStatus() {
        if (worker.isHealthServer()) {
            return "UP";
        } else {
            return "DOWN";
        }
    }

}
