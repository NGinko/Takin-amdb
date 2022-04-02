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

package io.shulie.amdb.adaptors.connector;

import io.shulie.amdb.adaptors.instance.model.InstanceModel;
import io.shulie.amdb.adaptors.instance.model.InstanceStatusModel;

import java.util.List;

/**
 * @author vincent
 */
public class DataContext<T> {
    /**
     *  agent监听:/config/log/pradar/client/
     *  simulator监听:/config/log/pradar/status/
     */
    /**
     * 实例：
     * 最开始定义的路径: /config/log/pradar/client/
     * 向下一级: /config/log/pradar/client/${appName}
     * 最终级(注册的节点):  /config/log/pradar/client/${appName}/${agentId}
     */

    //路径：目录 或 最总节点
    private String path;

    //子路径: 目录 或 最总节点
    private List<String> childPaths;

    /**
     * 保存zk中相关状态信息
     * 两个对应的实例
     * {@link InstanceModel}
     * {@link InstanceStatusModel}
     */
    private T model;

    public T getModel() {
        return model;
    }

    public void setModel(T model) {
        this.model = model;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<String> getChildPaths() {
        return childPaths;
    }

    public void setChildPaths(List<String> childPaths) {
        this.childPaths = childPaths;
    }

    @Override
    public String toString() {
        return "DataContext{" +
                "path='" + path + '\'' +
                ", model=" + model +
                '}';
    }
}
