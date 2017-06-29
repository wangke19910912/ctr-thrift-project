package com.codelab;

import com.codelab.common.utils.StringUtil;
import com.codelab.managers.ConstantManager;
import com.codelab.managers.DataManager;
import com.codelab.managers.FeatureManager;
import com.codelab.managers.ModelManager;
import com.codelab.util.ICommon;
import com.codelab.util.ZkAppConfig;
import com.codelab.util.ZkAppConfigForStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdPredict {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdPredict.class);

    private static class SingletonHolder {
        private static final AdPredict instance = new AdPredict();
    }

    public static final AdPredict getInstance() {
        return SingletonHolder.instance;
    }

    private static ZkAppConfig zkAppConfig = ZkAppConfig.getInstance();
    private static ZkAppConfigForStatic zkAppConfigStatic = ZkAppConfigForStatic.getInstance();

    private AdPredict() {
        LOGGER.info("AdPredict init!");

        //read from zk path
        String configData = null;
        try {
            configData = zkAppConfig.getData();
        } catch (Exception e) {
            LOGGER.error("zookeeper read error {}", e);
            System.out.printf("zookeeper read error %s\n", e.toString());
            System.exit(-1);
        }
        if (!ConstantManager.getInstance().init(configData)) {
            LOGGER.error("ConstantManager init failed , config path : {}", configData);
            System.out.printf("ConstantManager init failed , config path : %s\n", configData);
            System.exit(-1);
        }
        if (!DataManager.getInstance().init(configData)) {
            LOGGER.error("DataManager init failed , config path : {}", configData);
            System.out.printf("DataManager init failed , config path :  %s\n", configData);
            System.exit(-1);
        }
        if (!FeatureManager.getInstance().init(configData)) {
            LOGGER.error("FeatureManager init failed , config path : {}", configData);
            System.out.printf("FeatureManager init failed , config path :  %s\n", configData);
            System.exit(-1);
        }
        if (!ModelManager.getInstance().init(configData)) {
            LOGGER.error("ModelManager init failed , config path : {}", configData);
            System.out.printf("ModelManager init failed , config path :  %s\n", configData);
            System.exit(-1);
        }
    }


    public Map<Long, List<Double>> getScores(Map<String, List<String>> clientInfo, List<Long> retrieveAds, String expId, AdInfoRetrieve.RetrieveLevel retrieveLevel) {
        LOGGER.debug("getScores start ...");
        Map<Long, List<Double>> ctrMap = new HashMap<Long, List<Double>>();

        if (StringUtil.isEmpty(expId) || retrieveAds == null || retrieveAds.isEmpty() || clientInfo == null) {
            LOGGER.warn("receive user request not full info, some null: expid {}, retrieveAds: {}, clientInfo {}", expId, retrieveAds, clientInfo);
            return ctrMap;
        }

        if (!ModelManager.getInstance().expMap.containsKey(expId)) {
            LOGGER.warn("expId: {} not exist, return null", expId);
            return ctrMap;
        }

        Long requestTime = System.currentTimeMillis();

        String imei = ThriftClientInfoV3Helper.getImei(clientInfo);
        String tagId = ThriftClientInfoV3Helper.getTagId(clientInfo);
        ClientInfoV3 cv3 = ThriftClientInfoV3Helper.getClientInfoV3(clientInfo);

        expId = expIdConvert(cv3, tagId, expId);

        List<AdData> adsData = AdInfoRetrieve.retrive(retrieveAds, expId, retrieveLevel);
        ICommon parameters = ConstantManager.getInstance().getData(expId);

        ContextData contextData = getContextData(clientInfo, expId, parameters);
        UserData userData = getUserData(clientInfo, expId, parameters, imei);
        HistoricalDataCorpus histData = getHistoricalDataCorpus(clientInfo, expId, parameters);
        AdScoreAdjusterCorpus adScoreAdjusterCorpus = getAdScoreAdjusterCorpus(clientInfo, expId, parameters);

        Long time = System.currentTimeMillis();
        OnlineAlgorithm.select(contextData, userData, histData, adsData, expId, ctrMap, requestTime, adScoreAdjusterCorpus, retrieveLevel);
        OnlineAlgorithm.predictScores(contextData, userData, histData, adsData, expId, ctrMap, requestTime, adScoreAdjusterCorpus, retrieveLevel, TypeCollection.Calculation.SCORING);
        //OnlineAlgorithm.adjust(ctrMap, mediaType);
        //}

        StringBuilder sb = new StringBuilder(256);
        sb.append("expId ").append(expId).append(" ,user data ").append(userData.getImei()).append(", candidates ").append(retrieveAds.size()).append(" tag_id: ")
                .append(tagId).append(", process time: ").append(System.currentTimeMillis() - time).append(", ret size: ").append(ctrMap.size());

        return ctrMap;
    }

    private ContextData getContextData(Map<String, List<String>> clientInfo, String expId, ICommon parameters) {
        ContextData contextData = DataManager.getInstance().getContextData(expId).getData(clientInfo, expId, parameters);
        return contextData;
    }

    private UserData getUserData(Map<String, List<String>> clientInfo, String expId, ICommon parameters, String imei) {
        Boolean loadFromHbase = Boolean.parseBoolean(parameters.getAll().get("LOAD_USER_DATA_FROM_HBASE"));
        return DataManager.getInstance().getUserData(expId).getData(imei, loadFromHbase);
    }

    private HistoricalDataCorpus getHistoricalDataCorpus(Map<String, List<String>> clientInfo, String expId, ICommon parameters) {
        HistoricalDataCorpus histData = DataManager.getInstance().getHistData(expId);
        return histData;
    }

    private AdScoreAdjusterCorpus getAdScoreAdjusterCorpus(Map<String, List<String>> clientInfo, String expId, ICommon parameters) {
        AdScoreAdjusterCorpus adScoreAdjusterCorpus = DataManager.getInstance().getAdScoreAdjusterData(expId);
        return adScoreAdjusterCorpus;
    }

    private String expIdConvert(ClientInfoV3 cv3, String tagId, String expId) {
        if (cv3 != null && tagId != null && cv3.isSetMediaType()) {
            String mt = cv3.getMediaType().toString();
            if (zkAppConfigStatic.GetStringSetValue(CommonConstant.STREAMMODEL_MT, "").contains(mt)) {
                if (zkAppConfigStatic.GetStringSetValue(CommonConstant.STREAMMODEL_EXP, "").contains(expId)) {
                    if (!zkAppConfigStatic.GetStringSetValue(CommonConstant.STREAMMODEL_TAGIDS, "").contains(tagId)) {
                        //LOGGER.info("expIdConvert {},{},{}", mt, tagId,
                        //        zkAppConfigStatic.GetStringValue(CommonConstant.STREAMMODEL_MT_TAG_EXP+expId, expId));
                        PerfCounter.count("expIdConvert.convert." + expId, 1L);
                        return zkAppConfigStatic.GetStringValue(CommonConstant.STREAMMODEL_MT_TAG_EXP + expId, expId);
                    }
                }
            }
        }
        return expId;
    }


    //by xuran Map<Long, List<Double>>转为Map<Long, Double> 如果List多个值 则只返回第一个值

}
