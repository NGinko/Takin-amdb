/*
 * Copyright 2021 Shulie Technology, Co.Ltd
 * Email: shulie@shulie.io
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.shulie.amdb.adaptors.starter;

import io.shulie.amdb.adaptors.Adaptor;
import io.shulie.amdb.adaptors.AdaptorTemplate;
import io.shulie.amdb.adaptors.base.AdaptorFactory;
import io.shulie.amdb.adaptors.base.DefaultAdaptorTemplate;

import java.util.Map;
import java.util.Set;

/**
 * comm:启动类的适配器工厂，会从META-INF.services目录下的接口名文件中加载对应的实现类(主要还是为了解耦吧
 */
public class DefaultFactoryAdaptorStarter extends AbstractAdaptorStarter {

    private AdaptorTemplate adaptorTemplate;
    private final Map<String, Object> config;
    private final AdaptorFactory adaptorFactory;

    public DefaultFactoryAdaptorStarter(Map<String, Object> config) {
        adaptorFactory = AdaptorFactory.getFactory();
        this.config = config;
    }

    protected void doStart() throws Exception {
        adaptorTemplate = new DefaultAdaptorTemplate();
        //AdaptorFactory通过ServiceLoader的方式初始化定义好的Adapter对象
        //对象主要时维护zk对应节点的监听状态
        /**
         * 两个路径分别对应agent和simulator两个模块
         * `/config/log/pradar/client/`
         * `/config/log/pradar/status/`
         */
        Set<Adaptor> adaptors = adaptorFactory.getAdaptors();
        //注入template
        for (Adaptor adaptor : adaptors) {
            adaptor.setAdaptorTemplate(adaptorTemplate);
            adaptor.addConfig(config);
            //添加连接器
            adaptor.addConnector();
        }

        //template 初始化。期间会初始化zkClient
        adaptorTemplate.init();
        //真正的注册上面的父路径，同时开启监听回调。也会注册子节点的监听器
        adaptors.forEach(Adaptor::registor);
        //不知道为什么调用两次init，初始化两次确保一定启动？
        adaptorTemplate.start();
    }

    @Override
    void afterStart() {

    }

    @Override
    void beforeStart() {

    }

    public void stop() throws Exception {
        adaptorTemplate.close();
    }
}
