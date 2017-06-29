package com.codelab.util;

import com..miliao.zookeeper.ZKClient;
import com..miliao.zookeeper.ZKFacade;
import com...ad.util.zookeeper.ZKProperties;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincent on 2016/11/21.
 */
public class ZkAppConfigForStatic {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ZkAppConfigForStatic.class);
    private static final String ZK_NODE_PATH = "/services/com...ad.predict.thrift.service.AdPredictService/AppConfigStatic";
    private static final int TIME_TO_CHECK_ZK_IN_SECONDS = 30;

    private static boolean initOK = false;
    private static ZKProperties dataProperties = new ZKProperties();
    //  定义配置内容更新服务的任务列表
    private List<Runnable> tasks = new ArrayList<Runnable>();

    private static ZkAppConfigForStatic instance = null;
    public static synchronized ZkAppConfigForStatic getInstance() {
        if ( null == instance ) {
            instance = new ZkAppConfigForStatic();
        }
        return instance;
    }

    private ZkAppConfigForStatic() {
        initTaskList();
        if (tasks.size() > 0) {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(tasks.size());
            for (Runnable task : tasks) {
                scheduledExecutorService.scheduleAtFixedRate(task, 0,
                        TIME_TO_CHECK_ZK_IN_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    public static boolean isInitOK() {
        return initOK;
    }

    private void initTaskList() {
        tasks.add(new ZKConfigUpdater());
    }

    public String GetStringValue(String name, String defaultValue) {
        String result = defaultValue;
        if (dataProperties != null) {
            if (dataProperties.containsKey(name)) result = dataProperties.getProperty(name);
        }
        return result;
    }

    public Set<Integer> GetIntSetValue(String name, String defaultValue)
    {
        String line = GetStringValue(name, defaultValue);
        if (StringUtils.isEmpty(line)) {
            return new HashSet<Integer>();
        }

        Set<Integer> results = new HashSet<Integer>();
        for(String token : line.split(","))
        {
            try{
                int tempResult = Integer.valueOf(token);
                results.add(tempResult);
            } catch (Exception exp)
            {
                LOGGER.warn("ZKAppConfig: Get Value Set error:" + token);
            }
        }
        return results;
    }

    public int GetIntValue(String name, int defaultValue) {
        int iResult = defaultValue;

        if (dataProperties != null) {
            String strValue = null;
            if (dataProperties.containsKey(name)) {
                strValue = dataProperties.getProperty(name);
                try {
                    int tempResult = Integer.valueOf(strValue);
                    iResult = tempResult;
                } catch (Exception exp) {
                    LOGGER.warn("ZKAppConfig: Get Value error:" + strValue);
                }
            }
        }
        return iResult;
    }


    public long GetLongValue(String name, long defaultValue){
        long lResult = defaultValue;

        if (dataProperties != null){
            String strValue = null;
            if (dataProperties.containsKey(name)){
                strValue = dataProperties.getProperty(name);
                try {
                    long tempValue = Long.parseLong(strValue);
                    lResult = tempValue;
                } catch (Exception ex){
                    LOGGER.warn("ZKAppconfig: Get Value error: {}", name);
                }
            }
        }

        return lResult;
    }

    public Set<String> GetStringSetValue(String name, String defaultValue) {
        String line = GetStringValue(name, defaultValue);
        if (StringUtils.isEmpty(line)) {
            return new HashSet<String>();
        }
        return new HashSet<String>(Arrays.asList(line.split(",")));
    }

    public Set<String> GetStringSetValue(String name) {
        return GetStringSetValue(name, "");
    }

    public List<String> GetStringListValue(String name, String defaultValue) {
        String line = GetStringValue(name, defaultValue);
        if (StringUtils.isEmpty(line)) {
            return new ArrayList<String>();
        }
        return Arrays.asList(line.split(","));
    }


    public boolean GetBoolValue(String name, boolean defaultValue) {
        boolean bResult = defaultValue;
        if (dataProperties != null) {
            String strValue = null;
            if (dataProperties.containsKey(name)) {
                strValue = dataProperties.getProperty(name);
                try {
                    boolean tempResult = Boolean.valueOf(strValue);
                    bResult = tempResult;
                } catch (Exception exp) {
                    LOGGER.warn("ZKAppConfig: Get Value error: {}", strValue);
                }
            }
        }
        return bResult;
    }

    public double GetDoubleValue(String name, double defaultValue) {
        double bResult = defaultValue;
        if (dataProperties != null) {
            String strValue = null;
            if (dataProperties.containsKey(name)) {
                strValue = dataProperties.getProperty(name);
                try {
                    double tempResult = Double.valueOf(strValue);
                    bResult = tempResult;
                } catch (Exception exp) {
                    LOGGER.warn("ZKAppConfig: Get Value error: {}", strValue);
                }
            }
        }
        return bResult;
    }

    public ZKClient getZkClient() {
        return ZKFacade.getClient();
    }

    private ZKProperties createPropertyFromData(String data) {
        ZKProperties result = null;
        try {
            ZKProperties p = new ZKProperties();
            InputStream inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"));
            p.load(inputStream);
            result = p;

            LOGGER.debug("result: {}", result);
        } catch (Exception exp) {
            LOGGER.error("Create Property From Data Error:" + exp);
        }
        return result;
    }

    private class ZKConfigUpdater implements Runnable {
        @Override
        public void run() {
            try {
                LOGGER.info("Start Update ZKConfig.");
                ZKClient client = getZkClient();

                if ((client != null)) {
                    LOGGER.debug("Real PATH: {}",  client.getRealPath(ZK_NODE_PATH));
                    //Get Data and Create Property Object

                    if (client.exists(ZK_NODE_PATH)) {
                        String data = client.getData(String.class, ZK_NODE_PATH);

                        LOGGER.debug("Found ZK NODE & Data is" + data);
                        if (!StringUtils.isEmpty(data)) {

                            LOGGER.debug("ZKConfig Content:" + data);
                            ZKProperties properties = createPropertyFromData(data);
                            if (properties != null) {
                                dataProperties = properties;
                            }
                        }
                    } else {
                        LOGGER.debug("Fail to find the Node");
                    }
                }
            } catch (Exception exp) {
                LOGGER.warn("Update ZK Config Exception: {}" , exp);
            }
            initOK = true;
        }
    }

}
