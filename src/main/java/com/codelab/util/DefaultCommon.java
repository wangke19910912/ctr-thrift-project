package com.codelab.util;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hongang on 2015/10/10.
 */
public class DefaultCommon implements ICommon{
    public static final Logger LOGGER = LoggerFactory.getLogger(CommonConstant.class);
    public String BIAS_FEATURE = "bias";
    //public String AD_INFO_HDFS_PATH = "stream_ctr_prediction/default_ad_info";
    //public String HIST_INFO_HDFS_PATH = "stream_ctr_prediction/default_historical_info";
    //public String AD_SCORE_ADJUSTER_PATH = "stream_ctr_prediction/default_ad_score_adjuster_info";
    public int MAX_ADS_TO_COMPUTE = 300;
    public double CPD_ADJUSTER_VALUE = 1;
    public double NEW_AD_BOOST_VALUE = 1.7;
    public boolean ENABLE_SELECTION = true;
    public boolean ENABLE_SELECTION_TOP_HIST = false;
    public boolean ENABLE_SELECTION_TOP_MODEL = false;
    public boolean LOAD_USER_DATA_FROM_HBASE = true;
    public int SELECTION_TOP_ADS_NUMBER_HIST = 60;
    public int SELECTION_TOP_ADS_NUMBER_MODEL = 60;
    public String SELECTION_DEDUP_LEVEL_HIST = "campaign";
    public String SELECTION_DEDUP_LEVEL_MODEL = "campaign";
    public int SELECTION_RANDOM_ADS_NUMBER = 0;
    public String MEDIA_ID = "";
    public String HIST_INFO_WINDOW_SIZE = "-168";
    public boolean PARALLEL_SCORE = true;
    public boolean SELECTION_ENABLE_CPC = true;
    public boolean SELECTION_ENABLE_CPD = false;
    public boolean SELECTION_ENABLE_UNKNOWN = false;
    public boolean ENABLE_ONLINE_ADHISTORY = false;
    public boolean ENABLE_ASYNC_PREDICT = false;
    public boolean GET_ADHISTORY_FROM_MEDIA = true;
    public boolean ENABLE_NEW_AD = false;
    public boolean ENABLE_AD_SCORE_ADJUSTER = false;
    public boolean ENABLE_DYNAMIC_ADS_COUNT = false;
    public int MAX_LATENCY = 30;
    public int DYNAMIC_ADS_BATCH_SIZE = 5;
    //public boolean ENABLE_ONLINE_ADINFO = false;

    public Map<String, String> getAll() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("BIAS_FEATURE", BIAS_FEATURE);
        //map.put("AD_INFO_HDFS_PATH", AD_INFO_HDFS_PATH);
        //map.put("HIST_INFO_HDFS_PATH", HIST_INFO_HDFS_PATH);
        //map.put("AD_SCORE_ADJUSTER_PATH", AD_SCORE_ADJUSTER_PATH);
        map.put("MAX_ADS_TO_COMPUTE", String.valueOf(MAX_ADS_TO_COMPUTE));
        map.put("CPD_ADJUSTER_VALUE",String.valueOf(CPD_ADJUSTER_VALUE));
        map.put("NEW_AD_BOOST_VALUE", String.valueOf(NEW_AD_BOOST_VALUE));
        map.put("ENABLE_SELECTION", String.valueOf(ENABLE_SELECTION));
        map.put("ENABLE_SELECTION_TOP_HIST", String.valueOf(ENABLE_SELECTION_TOP_HIST));
        map.put("ENABLE_SELECTION_TOP_MODEL", String.valueOf(ENABLE_SELECTION_TOP_MODEL));
        map.put("LOAD_USER_DATA_FROM_HBASE", String.valueOf(LOAD_USER_DATA_FROM_HBASE));
        map.put("SELECTION_TOP_ADS_NUMBER_HIST", String.valueOf(SELECTION_TOP_ADS_NUMBER_HIST));
        map.put("SELECTION_TOP_ADS_NUMBER_MODEL", String.valueOf(SELECTION_TOP_ADS_NUMBER_MODEL));
        map.put("SELECTION_DEDUP_LEVEL_HIST", SELECTION_DEDUP_LEVEL_HIST);
        map.put("SELECTION_DEDUP_LEVEL_MODEL", SELECTION_DEDUP_LEVEL_MODEL);
        map.put("SELECTION_RANDOM_ADS_NUMBER", String.valueOf(SELECTION_RANDOM_ADS_NUMBER));
        map.put("MEDIA_ID", MEDIA_ID);
        map.put("HIST_INFO_WINDOW_SIZE", HIST_INFO_WINDOW_SIZE);
        map.put("PARALLEL_SCORE", String.valueOf(PARALLEL_SCORE));
        map.put("SELECTION_ENABLE_CPC", String.valueOf(SELECTION_ENABLE_CPC));
        map.put("SELECTION_ENABLE_CPD", String.valueOf(SELECTION_ENABLE_CPD));
        map.put("SELECTION_ENABLE_UNKNOWN", String.valueOf(SELECTION_ENABLE_UNKNOWN));
        map.put("ENABLE_ONLINE_ADHISTORY", String.valueOf(ENABLE_ONLINE_ADHISTORY));
        map.put("ENABLE_ASYNC_PREDICT", String.valueOf(ENABLE_ASYNC_PREDICT));
        map.put("GET_ADHISTORY_FROM_MEDIA", String.valueOf(GET_ADHISTORY_FROM_MEDIA));
        map.put("ENABLE_NEW_AD", String.valueOf(ENABLE_NEW_AD));
        map.put("ENABLE_AD_SCORE_ADJUSTER", String.valueOf(ENABLE_AD_SCORE_ADJUSTER));
        map.put("ENABLE_DYNAMIC_ADS_COUNT", String.valueOf(ENABLE_DYNAMIC_ADS_COUNT));
        map.put("MAX_LATENCY", String.valueOf(MAX_LATENCY));
        map.put("DYNAMIC_ADS_BATCH_SIZE", String.valueOf(DYNAMIC_ADS_BATCH_SIZE));

        return map;
    }

    public Object getKey(Object key) {
        try {
            //LOGGER.debug("object key: {}", key.toString());
            Field f = this.getClass().getField(key.toString());
            //Field[] f = CommonConstant.class.getClass().getDeclaredFields();
            //
            LOGGER.debug("object : {} value : {}", f.getName(), f.get(this));
            //LOGGER.debug("object value: {}", );
            //return f.get(String.class).toString();
            return f.get(this);
        }
        catch (Exception e) {
            return new Object();
        }
    }

    public boolean basicInit(Object obj) {
        if (! (obj instanceof Element)) {
            return false;
        }
        Element element = (Element)obj;
        String value = element.elementTextTrim("BIAS_FEATURE");
        if (value != null) {
            BIAS_FEATURE = value;
        }
        LOGGER.debug("value: {}", BIAS_FEATURE);

        /*
        value = element.elementTextTrim("AD_INFO_HDFS_PATH");
        if (value != null) {
            AD_INFO_HDFS_PATH = value;
        }
        LOGGER.debug("value: {}", AD_INFO_HDFS_PATH);

        value = element.elementTextTrim("HIST_INFO_HDFS_PATH");
        if (value != null) {
            HIST_INFO_HDFS_PATH = value;
        }
        LOGGER.debug("value: {}", HIST_INFO_HDFS_PATH);

        value = element.elementTextTrim("AD_SCORE_ADJUSTER_PATH");
        if (value != null) {
            AD_SCORE_ADJUSTER_PATH = value;
        }
        LOGGER.debug("value: {}", AD_SCORE_ADJUSTER_PATH);
        */


        value = element.elementTextTrim("MAX_ADS_TO_COMPUTE");
        if (value != null) {
            MAX_ADS_TO_COMPUTE = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", MAX_ADS_TO_COMPUTE);

        value = element.elementTextTrim("CPD_ADJUSTER_VALUE");
        if (value != null) {
            CPD_ADJUSTER_VALUE = Double.parseDouble(value);
        }
        LOGGER.debug("value: {}", CPD_ADJUSTER_VALUE);

        value = element.elementTextTrim("NEW_AD_BOOST_VALUE");
        if (value != null) {
            NEW_AD_BOOST_VALUE = Double.parseDouble(value);
        }
        LOGGER.debug("value: {}", NEW_AD_BOOST_VALUE);

        value = element.elementTextTrim("ENABLE_SELECTION");
        if (value != null) {
            ENABLE_SELECTION = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_SELECTION);

        value = element.elementTextTrim("ENABLE_SELECTION_TOP_HIST");
        if (value != null) {
            ENABLE_SELECTION_TOP_HIST = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_SELECTION_TOP_HIST);

        value = element.elementTextTrim("ENABLE_SELECTION_TOP_MODEL");
        if (value != null) {
            ENABLE_SELECTION_TOP_MODEL = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_SELECTION_TOP_MODEL);
        value = element.elementTextTrim("SELECTION_TOP_ADS_NUMBER_HIST");
        if (value != null) {
            SELECTION_TOP_ADS_NUMBER_HIST = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", SELECTION_TOP_ADS_NUMBER_HIST);
        value = element.elementTextTrim("SELECTION_TOP_ADS_NUMBER_MODEL");
        if (value != null) {
            SELECTION_TOP_ADS_NUMBER_MODEL = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", SELECTION_TOP_ADS_NUMBER_MODEL);


        value = element.elementTextTrim("SELECTION_DEDUP_LEVEL_HIST");
        if (value != null) {
            SELECTION_DEDUP_LEVEL_HIST = value;
        }
        LOGGER.debug("value: {}", SELECTION_DEDUP_LEVEL_HIST);


        value = element.elementTextTrim("SELECTION_DEDUP_LEVEL_MODEL");
        if (value != null) {
            SELECTION_DEDUP_LEVEL_MODEL = value;
        }
        LOGGER.debug("value: {}", SELECTION_DEDUP_LEVEL_MODEL);

        value = element.elementTextTrim("MEDIA");
        if (value != null) {
            MEDIA_ID = value;
        }
        LOGGER.debug("value: {}", MEDIA_ID);
        value = element.elementTextTrim("HIST_INFO_WINDOW_SIZE");
        if (value != null) {
            HIST_INFO_WINDOW_SIZE = value;
        }
        LOGGER.debug("value: {}", HIST_INFO_WINDOW_SIZE);

        value = element.elementTextTrim("SELECTION_ENABLE_CPC");
        if (value != null) {
            SELECTION_ENABLE_CPC = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", SELECTION_ENABLE_CPC);

        value = element.elementTextTrim("SELECTION_ENABLE_CPD");
        if (value != null) {
            SELECTION_ENABLE_CPD = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", SELECTION_ENABLE_CPD);
        value = element.elementTextTrim("SELECTION_ENABLE_UNKNOWN");
        if (value != null) {
            SELECTION_ENABLE_UNKNOWN = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", SELECTION_ENABLE_UNKNOWN);
        value = element.elementTextTrim("ENABLE_ONLINE_ADHISTORY");
        if (value != null) {
            ENABLE_ONLINE_ADHISTORY = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_ONLINE_ADHISTORY);

        value = element.elementTextTrim("ENABLE_ASYNC_PREDICT");
        if (value != null) {
            ENABLE_ASYNC_PREDICT = Boolean.parseBoolean(value);
        }
        LOGGER.debug("ENABLE_ASYNC_PREDICT value: {}", ENABLE_ASYNC_PREDICT);

        value = element.elementTextTrim("GET_ADHISTORY_FROM_MEDIA");
        if (value != null) {
            GET_ADHISTORY_FROM_MEDIA = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", GET_ADHISTORY_FROM_MEDIA);

        value = element.elementTextTrim("ENABLE_NEW_AD");
        if (value != null) {
            ENABLE_NEW_AD = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_NEW_AD);
        value = element.elementTextTrim("ENABLE_AD_SCORE_ADJUSTER");
        if (value != null) {
            ENABLE_AD_SCORE_ADJUSTER = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_AD_SCORE_ADJUSTER);

        value = element.elementTextTrim("PARALLEL_SCORE");
        if (value != null) {
            PARALLEL_SCORE = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", PARALLEL_SCORE);

        value = element.elementTextTrim("LOAD_USER_DATA_FROM_HBASE");
        if (value != null) {
            LOAD_USER_DATA_FROM_HBASE = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", LOAD_USER_DATA_FROM_HBASE);

        value = element.elementTextTrim("ENABLE_DYNAMIC_ADS_COUNT");
        if (value != null) {
            ENABLE_DYNAMIC_ADS_COUNT = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_DYNAMIC_ADS_COUNT);

        value = element.elementTextTrim("MAX_ADS_TO_COMPUTE");
        if (value != null) {
            MAX_ADS_TO_COMPUTE = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", MAX_ADS_TO_COMPUTE);
        value = element.elementTextTrim("DYNAMIC_ADS_BATCH_SIZE");
        if (value != null) {
            DYNAMIC_ADS_BATCH_SIZE = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", DYNAMIC_ADS_BATCH_SIZE);
        value = element.elementTextTrim("SELECTION_RANDOM_ADS_NUMBER");
        if (value != null) {
            SELECTION_RANDOM_ADS_NUMBER = Integer.parseInt(value);
        }
        LOGGER.debug("value: {}", SELECTION_RANDOM_ADS_NUMBER);
        /*
        value = element.elementTextTrim("ENABLE_ONLINE_ADINFO");
        if (value != null) {
            ENABLE_ONLINE_ADINFO = Boolean.parseBoolean(value);
        }
        LOGGER.debug("value: {}", ENABLE_ONLINE_ADINFO);
        */




        LOGGER.info("common data init ok");
        return true;
    }

    public boolean init(Object obj) {
        try {
            if (!basicInit(obj)) {
                LOGGER.error("basic init error");
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("common data init error:{}", e.getMessage());
            return false;
        }
    }
}
