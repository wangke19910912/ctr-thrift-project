package com.codelab.managers;

import com.codelab.util.CommonConstant;
import com.codelab.util.ICommon;
import com.codelab.util.ZkAppConfig;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConstantManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConstantManager.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static ConstantManager constantManager = null;
    private Map<String, ICommon> commonMap;

    private ConstantManager() {
    }

    public static synchronized ConstantManager getInstance() {
        if (constantManager == null)
            constantManager = new ConstantManager();
        return constantManager;
    }

    public ICommon getData(String expId) {
        return commonMap.get(expId);
    }


    private void start() {
        final Runnable task = new Runnable() {
            public void run() {
                try {
                    String configData = ZkAppConfig.getData();
                    Map<String, ICommon> tempMap = getCommonData(configData);
                    if (tempMap == null || tempMap.isEmpty() || tempMap.size() != commonMap.size()) {
                        LOGGER.warn("getCommon return ret: {}, ignore", tempMap);
                        return;
                    }
                    commonMap = tempMap;
                } catch (Exception e) {
                    LOGGER.error("update adInfo fail", e);
                }
            }
        };
        scheduler.scheduleAtFixedRate(task, CommonConstant.COMMON_UPDATE_PERIOD, CommonConstant.COMMON_UPDATE_PERIOD, TimeUnit.SECONDS); //
    }

    private Map<String, ICommon> getCommonData(String configData) {
        Map<String, ICommon> ret = new HashMap<String, ICommon>();
        try {
            SAXReader reader = new SAXReader();
            if (configData.isEmpty()) {
                LOGGER.error("config data is empty");
                return ret;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));
            Element rootElement = document.getRootElement();
            for (Element element : (List<Element>) rootElement.elements()) {
                String expId = element.attribute("id").getValue();
                for (Element sub_element : (List<Element>) element.elements()) {
                    if (sub_element.getName().equals("commons")) {
                        ICommon common = (ICommon) Class.forName(sub_element.attribute("class").getValue()).newInstance();
                        if (!common.init(sub_element)) {
                            LOGGER.error("expId:{} , common init false", expId);
                            ret.clear();
                            return ret;
                        }
                        ret.put(expId, common);
                        LOGGER.debug("expId:{} , const new instance success", expId);
                    }
                }
            }
            return ret;
        } catch (Exception e) {
            LOGGER.error("parse common config error", e);
            ret.clear();
            return ret;
        }
    }


    private Map<String, ICommon> getCommonData(String configData, String expId) {
        Map<String, ICommon> ret = new HashMap<String, ICommon>();
        try {
            SAXReader reader = new SAXReader();
            if (configData.isEmpty()) {
                LOGGER.error("config data is empty");
                return ret;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));
            Element rootElement = document.getRootElement();
            for (Element element : (List<Element>) rootElement.elements()) {
                String pexpId = element.attribute("id").getValue();
                // 一个meida其实也是一个实验
                if (expId.equals(pexpId)) {
                    for (Element sub_element : (List<Element>) element.elements()) {
                        if (sub_element.getName().equals("commons")) {
                            LOGGER.debug("read expId : {}", expId);
                            ICommon common = (ICommon) Class.forName(sub_element.attribute("class").getValue()).newInstance();
                            if (!common.init(sub_element)) {
                                LOGGER.error("expId:{} , common init false", expId);
                                ret.clear();
                                return ret;
                            }
                            ret.put(expId, common);
                            LOGGER.debug("expId:{} , model new instance success", expId);
                        }
                    }
                }
            }
            return ret;
        } catch (Exception e) {
            LOGGER.error("parse common config error", e);
            ret.clear();
            return ret;
        }
    }

    // online use;
    public boolean init(String configData) {
        commonMap = getCommonData(configData);
        if (commonMap == null || commonMap.isEmpty())
            return false;
        else {
            start();
            return true;
        }
    }

    // offline use;
    public boolean init(String configData, String expId) {
        commonMap = getCommonData(configData, expId);
        if (commonMap == null || commonMap.isEmpty())
            return false;
        else {
            return true;
        }
    }
}
