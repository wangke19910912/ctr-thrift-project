package com.codelab.managers;

import com.codelab.data.AdDataCorpus;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    // 统一管理所有的数据访问，singleton模式。以expId为key
    private static final Logger LOGGER = LoggerFactory.getLogger(DataManager.class);
    private static DataManager dataManager;
    private class DataPack {
        Integer adCropusKey;
        Integer histCorpusKey;
        Integer contextCorpusKey;
        Integer adScoreAdjusterCorpusKey;
        Integer userDataCorpusKey;
        Integer appCorpus;
    }

    private Map<Integer, AdDataCorpus> adCropusMap = new ConcurrentHashMap<Integer, AdDataCorpus>();

    private Map<String, DataPack> expDataPack = new HashMap<String, DataPack>();

    private DataManager() {
    }

    public static synchronized DataManager getInstance() {
        if (dataManager == null) {
            dataManager = new DataManager();
        }
        return dataManager;
    }

    public boolean init(String configData, String expId) {
        SAXReader reader = new SAXReader();
        try {
            if (StringUtils.isEmpty(configData)) {
                LOGGER.error("config data is empty");
                return false;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));

            Element rootElement = document.getRootElement();
            for(Element element : (List<Element>)rootElement.elements()){
                String pexpId = element.attribute("id").getValue();
                DataPack dataPack = new DataPack();
                if (StringUtils.isEmpty(expId) || expId.equals(pexpId)) {
                    for (Element sub_element : (List<Element>) element.elements()) {
                        if (sub_element.getName().equals("corpus")) {
                            String adClass = sub_element.elementTextTrim("adClass");
                            String userClass = sub_element.elementTextTrim("userClass");
                            String contextClass = sub_element.elementTextTrim("contextClass");
                            String historicalClass = sub_element.elementTextTrim("historicalClass");
                            String scoreAdjusterClass = sub_element.elementTextTrim("scoreAdjusterClass");
                            String appClass = sub_element.elementTextTrim("appInfoClass");
                            String appPath = sub_element.elementTextTrim("APP_INFO_HDFS_PATH");
                            String adPath = sub_element.elementTextTrim("AD_INFO_HDFS_PATH");
                            String histPath = sub_element.elementTextTrim("HIST_INFO_HDFS_PATH");
                            String adAjusterPath = sub_element.elementTextTrim("AD_SCORE_ADJUSTER_PATH");

                            Integer key = (adClass + "-" + adPath).hashCode();
                            if (!adCropusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(adClass) && !StringUtils.isEmpty(adPath)) {
                                    AdDataCorpus adDataCorpus = (AdDataCorpus)Class.forName(adClass).newInstance();
                                    if (!adDataCorpus.init(adPath)) {
                                        LOGGER.error("AdDataCorpus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("AdDataCorpus init success {}", key);
                                    adCropusMap.put(key, adDataCorpus);
                                }
                                else {
                                    LOGGER.error("AdDataCorpus init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.adCropusKey = key;

                            key = (historicalClass + "-" + histPath).hashCode();
                            if (!histCorpusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(historicalClass) && !StringUtils.isEmpty(histPath)) {
                                    HistoricalDataCorpus historicalDataCorpus = (HistoricalDataCorpus)Class.forName(historicalClass).newInstance();
                                    if (!historicalDataCorpus.init(histPath)) {
                                        LOGGER.error("HistDataCorpus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("HistDataCorpus init success {}", key);
                                    histCorpusMap.put(key, historicalDataCorpus);
                                }
                                else {
                                    LOGGER.error("HistDataCorpus init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.histCorpusKey = key;

                            key = (scoreAdjusterClass + "-" + adAjusterPath).hashCode();
                            if (!adScoreAdjusterCorpusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(scoreAdjusterClass) && !StringUtils.isEmpty(adAjusterPath)) {
                                    AdScoreAdjusterCorpus adScoreAdjusterCorpus = (AdScoreAdjusterCorpus) Class.forName(scoreAdjusterClass).newInstance();
                                    if (!adScoreAdjusterCorpus.init(adAjusterPath)) {
                                        LOGGER.error("AdScoreAdjusterCropus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("AdScoreAdjusterCropus init success {}", key);
                                    adScoreAdjusterCorpusMap.put(key, adScoreAdjusterCorpus);
                                }
                                else {
                                    LOGGER.error("AdScoreAdjusterCropus init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.adScoreAdjusterCorpusKey = key;

                            key = (userClass).hashCode();
                            if (!userDataCorpusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(userClass)) {
                                    UserDataCorpus userDataCorpus = (UserDataCorpus) Class.forName(userClass).newInstance();
                                    if (!userDataCorpus.init()) {
                                        LOGGER.error("userDataCorpus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("UserDataCropus init success {}", key);
                                    userDataCorpusMap.put(key, userDataCorpus);
                                }
                                else {
                                    LOGGER.error("UserDataCropus  init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.userDataCorpusKey = key;

                            key = (contextClass).hashCode();
                            if (!contextCorpusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(contextClass)) {
                                    ContextDataCorpus contextDataCorpus = (ContextDataCorpus) Class.forName(contextClass).newInstance();
                                    if (!contextDataCorpus.init()) {
                                        LOGGER.error("ContextDataCropus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("ContextDataCorpus init success {}", key);
                                    contextCorpusMap.put(key, contextDataCorpus);
                                }
                                else {
                                    LOGGER.error("ContextDataCropus init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.contextCorpusKey = key;

                            key = (appClass + "-" + appPath).hashCode();
                            if (!appInfoCorpusMap.containsKey(key)) {
                                if (!StringUtils.isEmpty(appClass) && !StringUtils.isEmpty(appPath)) {
                                    AppInfoCorpus appInfoCorpus = (AppInfoCorpus) Class.forName(appClass).newInstance();
                                    if (!appInfoCorpus.init(appPath)) {
                                        LOGGER.error("AppDataCorpus init failed {}", key);
                                        return false;
                                    }
                                    LOGGER.info("AppDataCorpus init success {}", key);
                                    appInfoCorpusMap.put(key, appInfoCorpus);
                                }
                                else {
                                    LOGGER.error("AppDataCorpus init failed {}", key);
                                    return false;
                                }
                            }
                            dataPack.appCorpus = key;

                        }
                    }
                }
                expDataPack.put(pexpId, dataPack);
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("DataManager init error:{}", e.getMessage());
            return false;
        }
    }

    public boolean init(String configData) {
        return init(configData, "");
    }
    public boolean containsKey(String expId) {
        return expDataPack.containsKey(expId);
    }

    public AdDataCorpus getAdData(String expId) {
        return adCropusMap.get(expDataPack.get(expId).adCropusKey);
    }

    public HistoricalDataCorpus getHistData(String expId) {
        return histCorpusMap.get(expDataPack.get(expId).histCorpusKey);
    }

    public AdScoreAdjusterCorpus getAdScoreAdjusterData(String expId) {
        return adScoreAdjusterCorpusMap.get(expDataPack.get(expId).histCorpusKey);
    }

    public ContextDataCorpus getContextData(String expId) {
        return contextCorpusMap.get(expDataPack.get(expId).contextCorpusKey);
    }

    public UserDataCorpus getUserData(String expId) {
        return userDataCorpusMap.get(expDataPack.get(expId).userDataCorpusKey);
    }

    public AppInfoCorpus getAppData(String expId) {
        return appInfoCorpusMap.get(expDataPack.get(expId).appCorpus);
    }
}
