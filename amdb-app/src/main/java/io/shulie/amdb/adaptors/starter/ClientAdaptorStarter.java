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

import io.shulie.amdb.service.AgentConfigService;
import io.shulie.amdb.service.AppInstanceService;
import io.shulie.amdb.service.AppInstanceStatusService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public class ClientAdaptorStarter extends DefaultFactoryAdaptorStarter {

    @Autowired
    private AppInstanceService appInstanceService;

    @Autowired
    private AppInstanceStatusService appInstanceStatusService;

    @Autowired
    private AgentConfigService agentConfigService;

    public ClientAdaptorStarter(Map<String, Object> config) {
        super(config);
    }

    @Override
    void afterStart() {

    }

    @Override
    public void beforeStart() {
        //solve problem that app instance flag synchronization failed when AMDB shutdown
        //step 1. set flag offline
        //step 2. acquire flag from zk ,ps: see InstanceAdaptor#process

        /**
         * 更新t_amdb_app_instance的flag，标记为1的修改为0
         */
        appInstanceService.initOnlineStatus();

        /**
         * 每次启动前将t_amdb_app_instance_status truncate
         * 所有的agent实例，重启之后清空了探针注册的记录所以可以修正应用的状态
         */
        appInstanceStatusService.truncateTable();

        /**
         * turn cate agent config (t_amdb_agent_config) 目前没信息？
         */
        agentConfigService.truncateTable();
    }
}
