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

package io.shulie.amdb.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.pamirs.pradar.log.parser.trace.RpcBased;
import io.shulie.amdb.common.Response;
import io.shulie.amdb.common.dto.trace.EntryTraceInfoDTO;
import io.shulie.amdb.common.enums.RpcType;
import io.shulie.amdb.common.request.trace.EntryTraceQueryParam;
import io.shulie.amdb.common.request.trace.TraceStackQueryParam;
import io.shulie.amdb.dao.ITraceDao;
import io.shulie.amdb.exception.AmdbExceptionEnums;
import io.shulie.amdb.service.TraceService;
import io.shulie.amdb.utils.StringUtil;
import io.shulie.surge.data.common.utils.Pair;
import io.shulie.surge.data.deploy.pradar.link.model.TTrackClickhouseModel;
import io.shulie.surge.data.deploy.pradar.parser.MiddlewareType;
import io.shulie.surge.data.deploy.pradar.parser.PradarLogType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

@Service
public class TraceServiceImpl implements TraceService {
    private Logger logger = LoggerFactory.getLogger(TraceServiceImpl.class);

    // trace????????????
    private static final String TRACE_SELECT_FILED
            = " appName,traceId,level,parentIndex,`index`,rpcId,rpcType,logType,traceAppName,upAppName,startTime,cost,"
            + "middlewareName,serviceName,methodName,remoteIp,port,resultCode,request,response,clusterTest,callbackMsg,"
            + "attributes,localAttributes,async,version,hostIp,agentId,parsedServiceName ";

    @Autowired
    @Qualifier("traceDaoImpl")
    ITraceDao traceDao;

    @Value("${config.trace.limit}")
    private String traceQueryLimit;

    @Override
    public Response<List<EntryTraceInfoDTO>> getEntryTraceInfo(EntryTraceQueryParam param) {
        Boolean e2eFlag = false;
        if (StringUtils.isNotBlank(param.getQuerySource()) && "e2e".equals(param.getQuerySource())) {
            e2eFlag = true;
        }

        if (!e2eFlag && StringUtils.isBlank(param.getAppName()) && StringUtils.isBlank(param.getEntranceList())) {
            return Response.fail(AmdbExceptionEnums.COMMON_EMPTY_PARAM_STRING_DESC, "appName and entranceList are all empty.");
        }

        // ??????????????????
        Pair<List<String>, List<String>> filters = getFilters(param, e2eFlag);
        List<String> andFilterList = filters.getFirst();
        List<String> orFilterList = filters.getSecond();
        if (isEmpty(andFilterList)) {
            return Response.fail(AmdbExceptionEnums.COMMON_EMPTY_PARAM_STRING_DESC, "??????????????????");
        }
        String sql = "select " + TRACE_SELECT_FILED + " from t_trace_all where " + StringUtils.join(
                andFilterList, " and ");
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            sql += " and (" + StringUtils.join(orFilterList, " or ") + ")";
        }
        sql += (e2eFlag ? " order by startTime desc " : " order by traceId desc ") + getLimitInfo(param);
        // ??????trace??????
        List<TTrackClickhouseModel> traceModelList = traceDao.queryForList(sql, TTrackClickhouseModel.class);

        if (e2eFlag) {
            Response result = Response.success(traceModelList.stream().map(model -> convert(model)).collect(Collectors.toList()));
            setResponseCount(andFilterList, orFilterList, result);
            return result;
        }

        Map<String, TTrackClickhouseModel> traceId2TraceMap = traceModelList.stream().collect(
                Collectors.toMap(TTrackClickhouseModel::getTraceId, model -> model, (k1, k2) -> k1));

        // ????????????????????????----???????????????????????????????????????appName???startDate?????????????????????
        Map<String, TTrackClickhouseModel> traceId2EngineTraceMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(traceModelList)) {
            String engineSql = "select " + TRACE_SELECT_FILED
                    + " from t_trace_all where traceId in ('"
                    + StringUtils.join(traceId2TraceMap.keySet(), "','")
                    + "') and logType='5' and appName='pressure-engine'";

            if (param.getStartTime() != null && param.getStartTime() > 0) {
                engineSql += " and startDate >= '"
                        + DateFormatUtils.format(new Date(param.getStartTime() + 5000), "yyyy-MM-dd HH:mm:ss")
                        + "'";
            }

            if (param.getEndTime() != null && param.getEndTime() > 0) {
                engineSql += " and startDate <= '"
                        + DateFormatUtils.format(new Date(param.getEndTime()), "yyyy-MM-dd HH:mm:ss")
                        + "'";
            }
            // ??????
            List<TTrackClickhouseModel> traceEngineModelList = traceDao.queryForList(engineSql,
                    TTrackClickhouseModel.class);
            traceId2EngineTraceMap = traceEngineModelList.stream().collect(
                    Collectors.toMap(TTrackClickhouseModel::getTraceId, model -> model, (k1, k2) -> k1));
        }

        // ????????????
        List<EntryTraceInfoDTO> entryTraceInfoDtos = mergeEngineTraceAndTrace(traceId2EngineTraceMap, traceId2TraceMap);
        Response<List<EntryTraceInfoDTO>> result = Response.success(entryTraceInfoDtos);
        setResponseCount(andFilterList, orFilterList, result);
        return result;
    }

    @Override
    public Response<List<EntryTraceInfoDTO>> getEntryTraceListByTaskId(EntryTraceQueryParam param) {
        // ??????????????????
        List<String> selectFields = getSelectFields(param.getFieldNames());
        if (isEmpty(selectFields)) {
            return Response.fail(AmdbExceptionEnums.TRACE_EMPTY_SELECT_FILED);
        }
        // ??????????????????
        Pair<List<String>, List<String>> filters = getFilters2(param);
        List<String> andFilterList = filters.getFirst();
        List<String> orFilterList = filters.getSecond();
        if (isEmpty(andFilterList)) {
            return Response.fail(AmdbExceptionEnums.COMMON_EMPTY_PARAM_STRING_DESC, "??????????????????");
        }

        // ??????
        String limit = getLimitInfo(param);
        // ??????????????????
        String sql = null;
        // ???????????????????????????????????????,??????????????????????????????
        if ("0".equals(param.getResultType()) || "2".equals(param.getResultType())) {
            sql = "select distinct" + TRACE_SELECT_FILED + " from t_trace_all where " + StringUtils.join(
                    andFilterList, " and ");
        } else {
            sql = "select " + TRACE_SELECT_FILED + " from t_trace_all where " + StringUtils.join(
                    andFilterList, " and ");
        }
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            sql += " and (" + StringUtils.join(orFilterList, " or ") + ")";
        }
        //1027 ???????????????????????????????????????
        sql += " order by cost desc " + limit;
        List<TTrackClickhouseModel> modelList = traceDao.queryForList(sql, TTrackClickhouseModel.class);
//        Map<String, TTrackClickhouseModel> traceId2EngineTraceMap = modelList.stream().collect(
//                Collectors.toMap(TTrackClickhouseModel::getTraceId, model -> model, (k1, k2) -> k1));
        // ??????????????????
        /*Set<String> traceIdSet = traceId2EngineTraceMap.keySet();
        Map<String, TTrackClickhouseModel> traceId2TraceMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(traceIdSet)) {
            param.setTraceIdList(traceIdSet);
            Pair<List<String>, List<String>> filters = getFilters(param);
            sql = "select " + TRACE_SELECT_FILED + " from t_trace_all where " + StringUtils.join(
                    filters.getFirst(), " and ");
            if (CollectionUtils.isNotEmpty(filters.getSecond())) {
                sql += " and (" + StringUtils.join(filters.getSecond(), " or ") + ")";
            }
            sql += " order by startDate asc ";
            modelList = clickhouseService.queryForList(sql, TTrackClickhouseModel.class);
            traceId2TraceMap = modelList.stream().collect(
                    Collectors.toMap(TTrackClickhouseModel::getTraceId, model -> model, (k1, k2) -> k1));
        }
        entryTraceInfoDtos = mergeEngineTraceAndTrace(traceId2EngineTraceMap, traceId2TraceMap);*/
        Response result = Response.success(modelList.stream().map(model -> convert(model)).collect(Collectors.toList()));
        if ("0".equals(param.getResultType()) || "2".equals(param.getResultType())) {
            setDistinctResponseCount(andFilterList, orFilterList, result);
        } else {
            setResponseCount(andFilterList, orFilterList, result);
        }
        return result;
    }


    public EntryTraceInfoDTO convert(TTrackClickhouseModel trackClickhouseModel) {
        EntryTraceInfoDTO entryTraceInfoDTO = new EntryTraceInfoDTO();
        entryTraceInfoDTO.setTraceId(trackClickhouseModel.getTraceId());
        entryTraceInfoDTO.setServiceName(trackClickhouseModel.getServiceName());
        entryTraceInfoDTO.setMethodName(trackClickhouseModel.getMethodName());
        entryTraceInfoDTO.setAppName(trackClickhouseModel.getAppName());
        entryTraceInfoDTO.setRemoteIp(trackClickhouseModel.getRemoteIp());
        entryTraceInfoDTO.setPort(trackClickhouseModel.getPort());
        entryTraceInfoDTO.setStartTime(trackClickhouseModel.getStartTime());
        entryTraceInfoDTO.setRequest(trackClickhouseModel.getRequest());
        entryTraceInfoDTO.setResultCode(trackClickhouseModel.getResultCode());
        entryTraceInfoDTO.setCost(trackClickhouseModel.getCost());
        entryTraceInfoDTO.setResponse(trackClickhouseModel.getResponse());
        entryTraceInfoDTO.setAssertResult(trackClickhouseModel.getCallbackMsg());
        return entryTraceInfoDTO;
    }


    /**
     * ???????????????????????????????????????
     *
     * @param traceId2EngineTraceMap ??????????????????
     * @param traceId2TraceMap       ??????trace??????
     */
    private List<EntryTraceInfoDTO> mergeEngineTraceAndTrace(Map<String, TTrackClickhouseModel> traceId2EngineTraceMap,
                                                             Map<String, TTrackClickhouseModel> traceId2TraceMap) {
        List<EntryTraceInfoDTO> entryTraceInfoDtos = new ArrayList<>();
        List<String> traceIdSet = new ArrayList<>();
        if (MapUtils.isNotEmpty(traceId2EngineTraceMap)) {
            traceIdSet.addAll(traceId2EngineTraceMap.keySet());
        }
        if (MapUtils.isNotEmpty(traceId2TraceMap)) {
            traceIdSet.addAll(traceId2TraceMap.keySet());
        }
        traceIdSet = traceIdSet.stream().distinct().collect(Collectors.toList());
        for (String traceId : traceIdSet) {
            EntryTraceInfoDTO entryTraceInfoDTO = new EntryTraceInfoDTO();
            TTrackClickhouseModel traceModel = traceId2TraceMap.get(traceId);
            TTrackClickhouseModel engineTraceModel = traceId2EngineTraceMap.get(traceId);
            if (traceModel != null) {
                entryTraceInfoDTO.setServiceName(traceModel.getParsedServiceName());
                entryTraceInfoDTO.setMethodName(traceModel.getMethodName());
                entryTraceInfoDTO.setAppName(traceModel.getAppName());
                entryTraceInfoDTO.setRemoteIp(traceModel.getRemoteIp());
                entryTraceInfoDTO.setPort(traceModel.getPort());
                entryTraceInfoDTO.setStartTime(traceModel.getStartTime());
                entryTraceInfoDTO.setRequest(traceModel.getRequest());
                entryTraceInfoDTO.setResultCode(traceModel.getResultCode());
                entryTraceInfoDTO.setCost(traceModel.getCost());
                entryTraceInfoDTO.setResponse(traceModel.getResponse());
                entryTraceInfoDTO.setAssertResult(traceModel.getCallbackMsg());
            } else if (engineTraceModel != null) {
                entryTraceInfoDTO.setServiceName(engineTraceModel.getServiceName());
                entryTraceInfoDTO.setMethodName(engineTraceModel.getMethodName());
                entryTraceInfoDTO.setAppName(engineTraceModel.getAppName());
                entryTraceInfoDTO.setRemoteIp(engineTraceModel.getRemoteIp());
                entryTraceInfoDTO.setPort(engineTraceModel.getPort());
                entryTraceInfoDTO.setStartTime(engineTraceModel.getStartTime());
                entryTraceInfoDTO.setRequest(engineTraceModel.getRequest());
            }
            if (engineTraceModel != null) {
                entryTraceInfoDTO.setRequest(engineTraceModel.getRequest());
                entryTraceInfoDTO.setResultCode(engineTraceModel.getResultCode());
                entryTraceInfoDTO.setCost(engineTraceModel.getCost());
                entryTraceInfoDTO.setResponse(engineTraceModel.getResponse());
                entryTraceInfoDTO.setAssertResult(engineTraceModel.getCallbackMsg());
            }
            entryTraceInfoDTO.setTraceId(traceId);
            entryTraceInfoDtos.add(entryTraceInfoDTO);
        }
        return entryTraceInfoDtos;
    }

    /**
     * ??????????????????
     *
     * @param fieldNameStr
     * @return
     */
    private List<String> getSelectFields(String fieldNameStr) {
        List<String> selectFields = new ArrayList<>();
        if (StringUtils.isBlank(fieldNameStr)) {
            return null;
        } else {
            List<String> fieldNames = Arrays.asList(fieldNameStr.split(","));
            if (fieldNames.contains("appName")) {
                selectFields.add("appName");
            }
            if (fieldNames.contains("remoteIp")) {
                selectFields.add("remoteIp");
            }
            if (fieldNames.contains("port")) {
                selectFields.add("port");
            }
            if (fieldNames.contains("resultCode")) {
                selectFields.add("resultCode");
            }
            if (fieldNames.contains("rpcType")) {
                selectFields.add("rpcType");
            }
            if (fieldNames.contains("cost")) {
                selectFields.add("cost");
            }
            if (fieldNames.contains("startTime")) {
                selectFields.add("startTime");
            }
            if (fieldNames.contains("traceId")) {
                selectFields.add("traceId");
            }
            if (fieldNames.contains("serviceName")) {
                selectFields.add("serviceName");
                selectFields.add("parsedServiceName");
            }
            if (fieldNames.contains("methodName")) {
                selectFields.add("methodName");
                selectFields.add("parsedMethod");
            }
        }
        return selectFields;
    }

    private Pair<List<String>, List<String>> getFilters(EntryTraceQueryParam param, Boolean e2eFlag) {
        List<String> andFilterList = new ArrayList<>();
        List<String> orFilterList = new ArrayList<>();
        if (StringUtils.isNotBlank(param.getAppName())) {
            if (param.getAppName().contains(",")) {
                StringBuilder sbuilder = new StringBuilder();
                sbuilder.append("appName in (");
                for (String app : param.getAppName().split(",")) {
                    sbuilder.append("'");
                    sbuilder.append(app);
                    sbuilder.append("'");
                    sbuilder.append(",");
                }
                sbuilder.deleteCharAt(sbuilder.lastIndexOf(","));
                sbuilder.append(")");
                andFilterList.add(sbuilder.toString());
            } else {
                andFilterList.add("appName='" + param.getAppName() + "'");
            }
        }
        if (StringUtils.isNotBlank(param.getRpcType())) {
            // web server
            if (param.getRpcType().equals(RpcType.TYPE_WEB_SERVER + "")) {
                andFilterList.add("rpcType='" + RpcType.TYPE_WEB_SERVER + "'");
            }
            // dubbo ?????? MQ ?????????????????????
            else if (param.getRpcType().equals(RpcType.TYPE_RPC + "") || param.getRpcType().equals(
                    RpcType.TYPE_MQ + "")) {
                andFilterList.add("rpcType='" + RpcType.TYPE_WEB_SERVER + "'");
                andFilterList.add("logType='" + PradarLogType.LOG_TYPE_RPC_SERVER + "'");
            }
            // ????????????????????????
            else {
                andFilterList.add("1 = -1");
            }
        } else {
            //????????????rpcType???????????????????????????tro(?????????),?????????????????????????????????,?????????e2e,??????????????????????????????
            boolean b = e2eFlag ? andFilterList.add("(logType in ('1','3','5'))") : andFilterList.add("(logType='1' or logType='3')");
        }

        if (StringUtils.isNotBlank(param.getMethodName())) {
            andFilterList.add("parsedMethod='" + param.getMethodName() + "'");
        }

        //?????????e2e??????,??????cost??????????????????
        if (e2eFlag && param.getMinCost() >= 0) {
            if (param.getMinCost() > 0 && param.getMaxCost() == 0) {
                andFilterList.add("cost >= " + param.getMinCost());
            } else if (param.getMaxCost() > 0 && param.getMaxCost() >= param.getMinCost()) {
                andFilterList.add("cost between " + param.getMinCost() + " and " + param.getMaxCost());
            }
        }

        if (StringUtils.isNotBlank(param.getServiceName())) {
            boolean b = e2eFlag ? andFilterList.add("parsedServiceName like '%" + param.getServiceName() + "%'") : andFilterList.add("parsedServiceName='" + param.getServiceName() + "'");
        }

        if (StringUtils.isNotBlank(param.getEntranceList())) {
            List<String> entryList = Arrays.asList(param.getEntranceList().split(","));
            entryList.forEach(entrance -> {
                String[] entranceInfo = entrance.split("#");
                if (StringUtils.isNotBlank(entranceInfo[0])) {
                    orFilterList.add("(appName='" + entranceInfo[0] + "' and parsedServiceName='" + entranceInfo[1]
                            + "' and parsedMethod='" + entranceInfo[2] + "' and rpcType='" + entranceInfo[3] + "')");
                }
            });
        }
        if (StringUtils.isNotBlank(param.getResultType())) {
            if ("1".equals(param.getResultType())) {
                andFilterList.add("(resultCode='00' or resultCode='200')");
            }
            if ("0".equals(param.getResultType())) {
                andFilterList.add("(resultCode<>'00' and resultCode<>'200')");
            }
        }
        if (StringUtils.isNotBlank(param.getClusterTest())) {
            andFilterList.add("clusterTest='" + param.getClusterTest() + "'");
        }
        if (param.getStartTime() != null && param.getStartTime() > 0) {
            andFilterList.add(
                    "startDate >= '" + DateFormatUtils.format(new Date(param.getStartTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        if (param.getEndTime() != null && param.getEndTime() > 0) {
            andFilterList.add(
                    "startDate <= '" + DateFormatUtils.format(new Date(param.getEndTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        //??????????????????/??????????????????,???????????????????????????
        if (e2eFlag && param.getStartTime() == null && param.getEndTime() == null) {
            Date now = new Date();
            andFilterList.add(
                    "startDate >= '" + DateFormatUtils.format(now.getTime() - 3600 * 1000, "yyyy-MM-dd HH:mm:ss") + "'");
            andFilterList.add(
                    "startDate <= '" + DateFormatUtils.format(now, "yyyy-MM-dd HH:mm:ss") + "'");
        }
        if (CollectionUtils.isNotEmpty(param.getTraceIdList())) {
            andFilterList.add("traceId in ('" + StringUtils.join(param.getTraceIdList(), "','") + "')");
        }
        if (StringUtils.isNotBlank(param.getTenantAppKey())) {
            andFilterList.add("userAppKey='" + param.getTenantAppKey() + "'");
        }
        if (StringUtils.isNotBlank(param.getEnvCode())) {
            andFilterList.add("envCode='" + param.getEnvCode() + "'");
        }
        return new Pair<>(andFilterList, orFilterList);
    }

    private Pair<List<String>, List<String>> getFilters2(EntryTraceQueryParam param) {
        List<String> andFilterList = new ArrayList<>();
        List<String> orFilterList = new ArrayList<>();

        if (param.getStartTime() != null && param.getStartTime() > 0) {
            andFilterList.add(
                    "startDate >= '" + DateFormatUtils.format(new Date(param.getStartTime() + 5000), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        if (param.getEndTime() != null && param.getEndTime() > 0) {
            andFilterList.add(
                    "startDate <= '" + DateFormatUtils.format(new Date(param.getEndTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }

        //?????????????????????,????????????????????????
        if ("debug".equals(param.getQuerySource())) {
            StringBuilder stringBuilder = new StringBuilder();
            if (StringUtils.isNotBlank(param.getEntranceList())) {
                List<String> entryList = Arrays.asList(param.getEntranceList().split(","));
                //????????????????????????????????????,?????????????????????,??????????????????????????????????????????/?????????????????????????????????????????????????????????
                String[] entranceInfo = entryList.get(0).split("#");
                if (entranceInfo.length == 4 && StringUtils.isNotBlank(entranceInfo[1])) {
                    //?????????????????????,??????????????????,????????????????????????
                    if (entranceInfo[1].contains("{")) {
                        stringBuilder.append("parsedServiceName= '" + entranceInfo[1] + "'");
                    } else {
                        stringBuilder.append("parsedServiceName like '%" + entranceInfo[1] + "%'");
                    }
                    if (StringUtils.isNotBlank(entranceInfo[2])) {
                        stringBuilder.append(" and parsedMethod='" + entranceInfo[2] + "'");
                    }
                    if (StringUtils.isNotBlank(entranceInfo[3])) {
                        stringBuilder.append(" and rpcType='" + entranceInfo[3] + "'");
                    }
                    andFilterList.add(stringBuilder.toString());
                }
            }
        }

        andFilterList.add("taskId='" + param.getTaskId() + "'");
        if (StringUtils.isNotBlank(param.getResultType())) {
            if ("2".equals(param.getResultType())) {
                andFilterList.add("resultCode='05'");
            }
            if ("1".equals(param.getResultType())) {
                andFilterList.add("(resultCode='00')");
            }
            if ("0".equals(param.getResultType())) {
                andFilterList.add("(resultCode<>'00' and resultCode<>'05')");
            }
        }
        return new Pair<>(andFilterList, orFilterList);
    }

    private Pair<List<String>, List<String>> getFilters3(EntryTraceQueryParam param) {
        List<String> andFilterList = new ArrayList<>();
        List<String> orFilterList = new ArrayList<>();

        if (param.getStartTime() != null && param.getStartTime() > 0) {
            andFilterList.add(
                    "startDate >= '" + DateFormatUtils.format(new Date(param.getStartTime() + 5000), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        if (param.getEndTime() != null && param.getEndTime() > 0) {
            andFilterList.add(
                    "startDate <= '" + DateFormatUtils.format(new Date(param.getEndTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }

        //???????????????????????????,????????????????????????????????????
        if (StringUtils.isBlank(param.getAppName())) {
            andFilterList.add("appName in ('" + StringUtils.join(param.getAppNames(), "','") + "')");
        }

        if (StringUtils.isNotBlank(param.getAppName())) {
            andFilterList.add("appName='" + param.getAppName() + "'");
        }
        if (StringUtils.isNotBlank(param.getServiceName())) {
            andFilterList.add("parsedServiceName='" + param.getServiceName() + "'");
        }
        if (StringUtils.isNotBlank(param.getMethodName())) {
            andFilterList.add("parsedMethod='" + param.getMethodName() + "'");
        }
        if (CollectionUtils.isNotEmpty(param.getTraceIdList())) {
            andFilterList.add("traceId in ('" + StringUtils.join(param.getTraceIdList(), "','") + "')");
        }
        if (StringUtils.isNotBlank(param.getTenantAppKey())) {
            andFilterList.add("userAppKey='" + param.getTenantAppKey() + "'");
        }
        if (StringUtils.isNotBlank(param.getEnvCode())) {
            andFilterList.add("envCode='" + param.getEnvCode() + "'");
        }
        return new Pair<>(andFilterList, orFilterList);
    }

    private String getLimitInfo(EntryTraceQueryParam param) {
        String limit = "";
        if ((param.getPageSize() != null && param.getPageSize() > 0) || (param.getCurrentPage() != null
                && param.getCurrentPage() > 0)) {
            int pageSize = param.getPageSize();
            if (pageSize <= 0) {
                pageSize = 20;
            }
            int currentPage = param.getCurrentPage();
            if (currentPage <= 0) {
                currentPage = 1;
            }
            limit = "limit " + ((currentPage - 1) * pageSize) + "," + pageSize;
        }
        return limit;
    }

    private void setResponseCount(List<String> andFilterList, List<String> orFilterList, Response response) {
        String countSql = "select count(1) as total " + " from t_trace_all where " + StringUtils.join(andFilterList,
                " and ");
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            countSql += " and (" + StringUtils.join(orFilterList, " or ") + ")";
        }
        Map<String, Object> countInfo = traceDao.queryForMap(countSql);
        long total = NumberUtils.toLong("" + countInfo.get("total"), 0);
        response.setTotal(total);
    }

    private void setDistinctResponseCount(List<String> andFilterList, List<String> orFilterList, Response response) {
        String countSql = "select count(1) as total " + " from ( select distinct " + TRACE_SELECT_FILED + " from t_trace_all where " + StringUtils.join(andFilterList,
                " and ");
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            countSql += " and (" + StringUtils.join(orFilterList, " or ") + "))";
        } else {
            countSql += ") t";
        }
        Map<String, Object> countInfo = traceDao.queryForMap(countSql);
        long total = NumberUtils.toLong("" + countInfo.get("total"), 0);
        response.setTotal(total);
    }

    @Override
    public Response<Map<String, List<RpcBased>>> getTraceInfo(EntryTraceQueryParam param) {

        // ??????????????????
        List<String> andFilterList = new ArrayList<>();
        List<String> orFilterList = new ArrayList<>();

        if (StringUtils.isNotBlank(param.getAppName())) {
            andFilterList.add("appName='" + param.getAppName() + "'");
        }
        if (StringUtils.isNotBlank(param.getMethodName())) {
            andFilterList.add("parsedMethod='" + param.getMethodName() + "'");
        }
        if (StringUtils.isNotBlank(param.getServiceName())) {
            andFilterList.add("parsedServiceName='" + param.getServiceName() + "'");
        }
        if (StringUtils.isNotBlank(param.getEntranceList())) {
            List<String> entryList = Arrays.asList(param.getEntranceList().split(","));
            entryList.forEach(entrance -> {
                String[] entranceInfo = entrance.split("#");
                if (StringUtils.isNotBlank(entranceInfo[0])) {
                    orFilterList.add("(appName='" + entranceInfo[0] + "' and parsedServiceName='" + entranceInfo[1]
                            + "' and parsedMethod='" + entranceInfo[2] + "' and rpcType='" + entranceInfo[3] + "')");
                }
            });
        }
        if (StringUtils.isNotBlank(param.getResultType())) {
            if ("1".equals(param.getResultType())) {
                andFilterList.add("(resultCode='00' or resultCode='200')");
            }
            if ("0".equals(param.getResultType())) {
                andFilterList.add("(resultCode<>'00' and resultCode<>'200')");
            }
        }
        if (StringUtils.isNotBlank(param.getClusterTest())) {
            andFilterList.add("clusterTest='" + param.getClusterTest() + "'");
        }
        if (param.getStartTime() != null && param.getStartTime() > 0) {
            andFilterList.add(
                    "startDate >= '" + DateFormatUtils.format(new Date(param.getStartTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        if (param.getEndTime() != null && param.getEndTime() > 0) {
            andFilterList.add(
                    "startDate <= '" + DateFormatUtils.format(new Date(param.getEndTime()), "yyyy-MM-dd HH:mm:ss") + "'");
        }
        // ??????
        String limit = "";
        if ((param.getPageSize() != null && param.getPageSize() > 0) || (param.getCurrentPage() != null
                && param.getCurrentPage() > 0)) {
            int pageSize = param.getPageSize();
            if (pageSize <= 0) {
                pageSize = 20;
            }
            int currentPage = param.getCurrentPage();
            if (currentPage <= 0) {
                currentPage = 1;
            }
            limit = "limit " + ((currentPage - 1) * pageSize) + "," + pageSize;
        } else {
            limit = "limit 10";
        }
        String sql = "select traceId from t_trace_all where " + StringUtils.join(
                andFilterList, " and ");
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            sql += " and (" + StringUtils.join(orFilterList, " or ") + ")";
        }
        String countSql = "select count(1) as total " + " from t_trace_all where " + StringUtils.join(andFilterList,
                " and ");
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            countSql += " and (" + StringUtils.join(orFilterList, " or ") + ")";
        }
        sql += " order by traceId desc " + limit;
        List<Map<String, Object>> modelList = traceDao.queryForList(sql);
        Map<String, List<RpcBased>> traceMap = new HashMap<>();
        for (Map<String, Object> map : modelList) {
            String traceId = String.valueOf(map.get("traceId"));
            TraceStackQueryParam tmp = new TraceStackQueryParam();
            tmp.setTraceId(traceId);
            traceMap.put(traceId, getTraceDetail(tmp));
        }
        Map<String, Object> countInfo = traceDao.queryForMap(countSql);
        long total = NumberUtils.toLong("" + countInfo.get("total"), 0);
        Response result = Response.success(traceMap);
        result.setTotal(total);
        return result;
    }

    @Override
    public List<Map<String, Object>> queryInterfaceParam(TraceStackQueryParam param) {
        //????????????
        int pageSize = 1000;

        StringBuilder sql = new StringBuilder();
        StringBuilder countSql = new StringBuilder();

        //???????????????,?????????????????????
        countSql.append("select count(1) as total from (select distinct request from t_trace_all where");
        if (StringUtil.isNotBlank(param.getStartTime()) && StringUtil.isNotBlank(param.getEndTime())) {
            countSql.append(" startDate between '" + param.getStartTime() + "' and '" + param.getEndTime() + "' and ");
        }
        countSql.append(" appName = '" + param.getAppName() + "' and parsedServiceName = '" + param.getServiceName() + "' and parsedMethod = '" + param.getMethodName() + "' and rpcType = '" + param.getRpcType() + "' and request not in ('','{}')) ");
        Map<String, Object> countInfo = traceDao.queryForMap(countSql.toString());
        long total = NumberUtils.toLong("" + countInfo.get("total"), 0);
        int page = (total / pageSize == 0) ? 1 : (int) (total % pageSize == 0 ? (total % pageSize) : total / pageSize + 1);

        for (int i = 1; i <= page; i++) {
            sql.append("select distinct request from t_trace_all where ");
            if (StringUtil.isNotBlank(param.getStartTime()) && StringUtil.isNotBlank(param.getEndTime())) {
                sql.append(" startDate between '" + param.getStartTime() + "' and '" + param.getEndTime() + "' and ");
            }
            sql.append(" appName = '" + param.getAppName() + "' and parsedServiceName = '" + param.getServiceName() + "' and parsedMethod = '" + param.getMethodName() + "' and rpcType = '" + param.getRpcType() + "' and request not in ('','{}') ");
            sql.append("limit " + ((i - 1) * pageSize) + "," + pageSize);
            List<TTrackClickhouseModel> modelList = traceDao.queryForList(sql.toString(), TTrackClickhouseModel.class);

            List<Map<String, Object>> resultList = new ArrayList<>();
            if (!modelList.isEmpty()) {
                modelList.stream().forEach(model -> {
                    String request = model.getRequest().trim();
                    //??????request??????{??????,}??????,???????????????{}
                    if (request.startsWith("{") && request.endsWith("}")) {
                        request = request.substring(1, request.length() - 1);
                    }
                    String temp = request;
                    try {
                        Map<String, Object> resultMap = JSONObject.parseObject(request);

                        temp = temp.substring(1, temp.length() - 1);
                        String[] entryArr = temp.split(",");
                        List<String> keyList = Lists.newArrayList();
                        for (String entry : entryArr) {
                            String key = entry.split(":")[0];
                            keyList.add(key.substring(1, key.length() - 1));
                        }
                        if (!keyList.isEmpty()) {
                            LinkedHashMap linkeResult = new LinkedHashMap();
                            keyList.forEach(key -> {
                                linkeResult.put(key, resultMap.get(key));
                            });
                            resultList.add(linkeResult);
                        }
                    } catch (Exception e) {
                        logger.warn("???json????????????:{}", request);

                        //???json??????
                        //???????????????
                    }
                });
            }

            return resultList;
        }

        //request
        return new ArrayList<>();
    }

    @Override
    public Response<List<TTrackClickhouseModel>> getAllTraceList(EntryTraceQueryParam param) {
        // ??????????????????
        Pair<List<String>, List<String>> filters = getFilters3(param);
        List<String> andFilterList = filters.getFirst();
        List<String> orFilterList = filters.getSecond();
        // ??????
        String limit = getLimitInfo(param);
        // ??????????????????
        StringBuilder sql = new StringBuilder("select * from t_trace_all where " + StringUtils.join(
                andFilterList, " and "));
        if (CollectionUtils.isNotEmpty(orFilterList)) {
            sql.append(" and (" + StringUtils.join(orFilterList, " or ") + ")");
        }
        //1027 ???????????????????????????????????????
        sql.append(limit);

        List<TTrackClickhouseModel> modelList = traceDao.queryForList(sql.toString(), TTrackClickhouseModel.class);
        Response result = null;
        if (!modelList.isEmpty()) {
            result = Response.success(modelList);
            StringBuilder countSql = new StringBuilder("select count(1) as total from t_trace_all where " + StringUtils.join(andFilterList, " and "));
            if (CollectionUtils.isNotEmpty(orFilterList)) {
                sql.append(" and (" + StringUtils.join(orFilterList, " or ") + ")");
            }
            Map<String, Object> countInfo = traceDao.queryForMap(countSql.toString());
            result.setTotal(NumberUtils.toLong("" + countInfo.get("total"), 0));
            return result;
        }

        result = Response.success(new ArrayList<>());
        result.setTotal(0);
        return result;
    }

    @Override
    public List<RpcBased> getTraceDetail(TraceStackQueryParam param) {
        StringBuilder sql = new StringBuilder();
        sql.append("select " + TRACE_SELECT_FILED + " from t_trace_all where 1=1 ");
        if (StringUtil.isNotBlank(param.getStartTime()) && StringUtil.isNotBlank(param.getEndTime())) {
            sql.append(" and startDate between '" + param.getStartTime() + "' and '" + param.getEndTime() + "' ");
        }
        if (StringUtils.isNotBlank(param.getTenantAppKey())) {
            sql.append(" and userAppKey='").append(param.getTenantAppKey()).append("' ");
        }
        if (StringUtils.isNotBlank(param.getEnvCode())) {
            sql.append(" and envCode='").append(param.getEnvCode()).append("' ");
        }
        sql.append(" and traceId='" + param.getTraceId()
                + "' order by rpcId limit " + traceQueryLimit);
        List<TTrackClickhouseModel> modelList = traceDao.queryForList(sql.toString(), TTrackClickhouseModel.class);
        /*List<TTrackClickhouseModel> engineModelList = modelList.stream().filter(model -> 5 == model.getLogType())
                .collect(Collectors.toList());*/
        /*TTrackClickhouseModel engineModel = null;
        if (CollectionUtils.isNotEmpty(engineModelList)) {
            modelList = subtractToList(modelList, engineModelList);
            engineModel = engineModelList.get(0);
        }*/

        // ??????????????????
        if (modelList.size() > 1) {
            modelList = modelList.stream().filter(model -> model.getLogType() != 5).collect(Collectors.toList());
        }
        for (TTrackClickhouseModel model : modelList) {
            // ???????????????????????????????????????
            if (model.getLogType() == 2) {
                calculateCost(model, modelList);
            }
            /*// ???????????????????????????????????????????????????????????????????????????response???request
            if (engineModel != null && (model.getLogType() == 1 || model.getLogType() == 3) && model
                    .getParsedServiceName().equals(
                            engineModel.getServiceName()) && model.getRpcType() == engineModel.getRpcType()) {
                model.setStartTime(engineModel.getStartTime());
                model.setCost(engineModel.getCost());
                model.setRequest(engineModel.getRequest());
                model.setResponse(engineModel.getResponse());
            }*/
        }
        List<RpcBased> rpcBasedList = modelList.stream().map(model -> model.getRpcBased()).collect(Collectors.toList());
        return rpcBasedList;
    }

    private void calculateCost(TTrackClickhouseModel clientModel, List<TTrackClickhouseModel> modelList) {
        TTrackClickhouseModel serverModel = modelList.stream().filter(
                        m -> m.getLogType() == 3 && m.getRpcId().startsWith(clientModel.getRpcId()) && m.getServiceName()
                                .equals(clientModel.getServiceName()) && m.getMethodName().equals(clientModel.getMethodName()))
                .findFirst().orElse(null);
        if (serverModel != null) {
            // ???MQ??????????????????????????????????????????????????????????????????????????????=???????????????+???????????????
            if (clientModel.getRpcType() != MiddlewareType.TYPE_MQ) {
                if (clientModel.getCost() < serverModel.getCost()) {
                    clientModel.setCost(clientModel.getCost() + serverModel.getCost());
                }
            }
        }
    }

    /**
     * ????????????????????????????????????????????????1???????????????????????????2?????????????????????????????????
     *
     * <pre>
     *     subtractToList([1,2,3,4],[2,3,4,5]) -??? [1]
     * </pre>
     *
     * @param coll1 ??????1
     * @param coll2 ??????2
     * @param <T>   ????????????
     * @return ?????????
     * @since 5.3.5
     */
    public static <T> List<T> subtractToList(Collection<T> coll1, Collection<T> coll2) {

        if (isEmpty(coll1)) {
            return Collections.emptyList();
        }
        if (isEmpty(coll2)) {
            return new LinkedList<>(coll1);
        }

        //??????????????????????????????????????????????????????????????????
        final List<T> result = new LinkedList<>();
        Set<T> set = new HashSet<>(coll2);
        for (T t : coll1) {
            if (false == set.contains(t)) {
                result.add(t);
            }
        }
        return result;
    }
}
