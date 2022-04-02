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


public abstract class AbstractAdaptorStarter implements IAdaptorStarter {

    public void start() throws Exception {
        /**
         * 更新amdb对应数据库中的状态信息:
         * 更新应用状态表和探针配置表(配置表目前没数据)
         */
        beforeStart();
        /**
         * 初始化zk链接，注册zk对应节点路径的监听器并开启
         */
        doStart();
        /**
         * 空方法，可以说没有实际内容
         */
        afterStart();
    }

    protected abstract void doStart() throws Exception;

    abstract void afterStart();

    abstract void beforeStart();

    public void restart() throws Exception {
        start();
        stop();
    }

}
