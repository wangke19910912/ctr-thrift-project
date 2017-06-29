package com.codelab.util;

import com..miliao.zookeeper.EnvironmentType;
import com..miliao.zookeeper.ZKClient;
import com..miliao.zookeeper.ZKFacade;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincent on 15/11/15.
 */
public class ZkAppConfig {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ZkAppConfig.class);
    //private static final String ZK_NODE_PATH = "/services/com...ad.yidian.ctr.thrift.service.YidianCTRPredictionService/AppConfig";
    //private static final int TIME_TO_CHECK_ZK_IN_SECONDS = 60;
    private static boolean initOK = false;
    private static Document zkDocument = null;
    private static String zkData = "";
    private List<Runnable> tasks = new ArrayList<Runnable>();

    public static boolean isInitOk() {
        return initOK;
    }

    private static class SingletonHolder {
        private static final ZkAppConfig instance = new ZkAppConfig();
    }

    public static final ZkAppConfig getInstance(){
        return SingletonHolder.instance;
    }

    private void initTaskList() {
        updateDocument();
        tasks.add(new ZkConfigUpdater());
    }

    private ZkAppConfig() {
        initTaskList();
        if (tasks.size() > 0) {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(tasks.size());
            for (Runnable task : tasks) {
                scheduledExecutorService.scheduleAtFixedRate(task, CommonConstant.ZK_UPDATE_PERIOD,
                        CommonConstant.ZK_UPDATE_PERIOD, TimeUnit.SECONDS);
            }
        }
    }

    public static String getData() {
        return zkData;
    }

    /*
    public static boolean getHistoryDataOnline(String mediaNo, String expId) {
        Element rootElement = zkDocument.getRootElement();
        for (Element element : (List<Element>) rootElement.elements()) {
            String eExpId = element.attribute("id").getValue();
            String eMedia = element.attribute("media").getValue();
            LOGGER.debug("media {}, expid {}", eExpId, eMedia);
            if (null != eExpId && (eExpId.equalsIgnoreCase(expId)
                    || !Character.isDigit(eExpId.charAt(0)))) {
                for (Element sub_element : (List<Element>) element.elements()) {
                    if (sub_element.getName().equals("commons")) {
                        String eEnableOnlineHistData = sub_element.elementTextTrim("ENABLE_ONLINE_ADHISTORY");
                        if (null != eMedia && eMedia.equalsIgnoreCase(mediaNo)) {
                            return "true".equalsIgnoreCase(eEnableOnlineHistData);
                        }
                    }
                }
            }
        }
        return false;
    }
    */

    private Document createPropertyFromData(String configData) {
        Document document = null;
        try {
            SAXReader reader = new SAXReader();
            document = reader.read(new InputSource(new StringReader(configData)));
        } catch (Exception e) {
            LOGGER.error("ZKAppConfig createPropertyFromData failed! Exception: {}", e.toString());
        } finally {
            return document;
        }
    }

    public ZKClient getZkClient() {
        return ZKFacade.getClient();
    }

    private void updateDocument() {
        try {
            LOGGER.info("Start Update ZKConfig.");
            ZKClient client = getZkClient();
            if ((client != null)) {
                String zkPath = CommonConstant.ZK_NODE_PATH;
                if (ZKFacade.getZKSettings().getEnvironmentType().equals(EnvironmentType.ONEBOX))
                {
                    try {
                        zkPath += ("_" + InetAddress.getLocalHost().getHostName());
                    }
                    catch (Exception e) {
                        LOGGER.error("Staging get host name error: {}", e.getMessage());
                        return;
                    }
                }
                LOGGER.debug("Real PATH: {}", client.getRealPath(zkPath));
                if (client.exists(zkPath)) {
                    String data = client.getData(String.class, zkPath);
                    LOGGER.debug("Found ZK NODE & Data is {}" + data);

                    if (!StringUtils.isEmpty(data)) {
                        LOGGER.debug("ZKConfig Content:" + data);
                        zkData = data;
                        Document document = createPropertyFromData(data);
                        if (document != null) {
                            zkDocument = document;
                        }
                    }
                } else {
                    LOGGER.warn("Fail to find the Node");
                }
            }
        } catch (Exception exp) {
            LOGGER.warn("Update ZK Config Exception: {}" , exp);
        }
    }

    private class ZkConfigUpdater implements Runnable {
        public void run() {
            updateDocument();
            initOK = true;
        }
    }
}
