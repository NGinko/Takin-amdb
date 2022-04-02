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

package io.shulie.amdb.adaptors.instance;

import com.google.common.collect.Sets;
import io.shulie.amdb.adaptors.AdaptorTemplate;
import io.shulie.amdb.adaptors.base.AbstractDefaultAdaptor;
import io.shulie.amdb.adaptors.connector.Connector;
import io.shulie.amdb.adaptors.connector.DataContext;
import io.shulie.amdb.adaptors.instance.model.InstanceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * 父实例扫描
 *
 * @author vincent
 */
public class InstanceParentAdaptor extends AbstractDefaultAdaptor {

    private static final Logger logger = LoggerFactory.getLogger(InstanceParentAdaptor.class);

    private AdaptorTemplate adaptorTemplate;
    private static final String INSTANCE_PATH = "/config/log/pradar/client";
    private Set<String> paths = Sets.newHashSet();

    @Override
    public void addConnector() {
        adaptorTemplate.addConnector(Connector.ConnectorType.ZOOKEEPER_PATH);
    }

    /**
     * 注册
     */
    @Override
    public void registor() {
        try {
            //Parent级别的都是zk_path的类型，表示的上级。具体探针节点注册的是zk_node
            adaptorTemplate.addPath(Connector.ConnectorType.ZOOKEEPER_PATH, INSTANCE_PATH, InstanceModel.class, this);
        } catch (Exception e) {
            logger.error("Adapter add path error.", e);
        }
    }


    @Override
    public void setAdaptorTemplate(AdaptorTemplate adaptorTemplate) {
        this.adaptorTemplate = adaptorTemplate;
    }

    @Override
    public boolean close() throws Exception {
        return false;
    }

    /**
     *process方法其实就是拼接了路径和子路径，最后向appName级的路径注册监听
     * 所以此方法调用的路径依次是：`/config/log/pradar/client/`,`/config/log/pradar/client/${appName}`
     */
    @Override
    public Object process(DataContext dataContext) {
        if (!paths.contains(dataContext.getChildPaths())) {
            for (Object path : dataContext.getChildPaths()) {
                if (!paths.contains(dataContext.getPath() + "/" + path)) {
                    try {
                        adaptorTemplate.addPath(Connector.ConnectorType.ZOOKEEPER_PATH, dataContext.getPath() + "/" + path, InstanceModel.class, adaptorTemplate.getAdapter(InstanceAppAdaptor.class));
                    } catch (Exception e) {
                        logger.error("Add path listener error");
                    }
                }
            }
        }
        return null;
    }

}
