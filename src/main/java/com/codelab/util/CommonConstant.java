package com.codelab.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.oracle.jrockit.jfr.ContentType.Bytes;

/**
 * Created by liyongbao on 15-8-18.
 */
public class CommonConstant {
    public static final Logger LOGGER = LoggerFactory.getLogger(CommonConstant.class);
    public static final String HBASE_TABLE_YIDIAN_USER = "mi_ad_user_data";

    // new hbase table
    public static final String HBASE_TABLE_USER_DATA = "hbase://c3srv-adsctr/_ad:_ad_algorithm_data";
    public static final String HBASE_TABLE_USER_DATA_NAME = "_ad:_ad_algorithm_data";

    public static final String HBASE_CONFIG_TABLE_USER = "_ad/predict_user_data";

    public static final String HBASE_CONFIG_TABLE_USER_NEW = "_ad/predict_user_data_0.98";

    public static final String ZK_NODE_PATH = "/AppConfig";

    public static final int ZK_UPDATE_PERIOD = 180;
    public static final int AD_CORPUS_UPDATE_PERIOD = 300;
    public static final int MODEL_UPDATE_PERIOD = 300;
    public static final int MAX_USER_COUNT = 300000;
    public static final int CACHE_EXPIRE_TIME = 3 * 60 * 60;
    public static final int CACHE_CONCUR_LEVEL = 8;
    public static final int COMMON_UPDATE_PERIOD = 180;
    public static final int HIST_UPDATE_PERIOD= 180;
    public static final int APP_CORPUS_UPDATE_PERIOD = 300;

    public static final String STREAMMODEL_MT = "STREAMMODEL_MT";
    public static final String STREAMMODEL_MT_TAG_EXP = "STREAMMODEL_MT_TAG_EXP.";
    public static final String STREAMMODEL_TAGIDS = "TAGIDS";
    public static final String STREAMMODEL_EXP = "STREAMMODEL_EXP";

    public static final String HBASE_LOCALCACHE_MAXUSER = "hbaseLocalCacheMaxUser";
    public static final String HBASE_LOCALCACHE_EXPTIME = "hbaseLocalCacheExpTime";
    public static final String HBASE_LOCALCACHE_CONCLEVEL = "hbaseLocalCacheConcLevel";


    
    public static Map<String, Integer> PROVINCE_ID = new HashMap<String, Integer>();
    static{
        PROVINCE_ID.put("UNKNOWN",0);
        PROVINCE_ID.put("BEIJING",1);
        PROVINCE_ID.put("北京",1);
        PROVINCE_ID.put("SHANGHAI",2);
        PROVINCE_ID.put("上海",2);
        PROVINCE_ID.put("TIANJIN",3);
        PROVINCE_ID.put("天津",3);
        PROVINCE_ID.put("CHONGQING",4);
        PROVINCE_ID.put("重庆",4);
        PROVINCE_ID.put("GUANGDONG",5);
        PROVINCE_ID.put("广东",5);
        PROVINCE_ID.put("ZHEJIANG",6);
        PROVINCE_ID.put("浙江",6);
        PROVINCE_ID.put("FUJIAN",7);
        PROVINCE_ID.put("福建",7);
        PROVINCE_ID.put("GUANGXI",8);
        PROVINCE_ID.put("广西",8);
        PROVINCE_ID.put("HAINAN",9);
        PROVINCE_ID.put("海南",9);
        PROVINCE_ID.put("HUNAN",10);
        PROVINCE_ID.put("湖南",10);
        PROVINCE_ID.put("HUBEI",11);
        PROVINCE_ID.put("湖北",11);
        PROVINCE_ID.put("JIANGXI",12);
        PROVINCE_ID.put("江西",12);
        PROVINCE_ID.put("JIANGSU",13);
        PROVINCE_ID.put("江苏",13);
        PROVINCE_ID.put("ANHUI",14);
        PROVINCE_ID.put("安徽",14);
        PROVINCE_ID.put("SHANXIJIN",15);
        PROVINCE_ID.put("山西",15);
        PROVINCE_ID.put("SHANXISHAN",16);
        PROVINCE_ID.put("陕西",16);
        PROVINCE_ID.put("HENAN",17);
        PROVINCE_ID.put("河南",17);
        PROVINCE_ID.put("HEBEI",18);
        PROVINCE_ID.put("河北",18);
        PROVINCE_ID.put("SICHUAN",19);
        PROVINCE_ID.put("四川",19);
        PROVINCE_ID.put("YUNNAN",20);
        PROVINCE_ID.put("云南",20);
        PROVINCE_ID.put("XIANGGANG",21);
        PROVINCE_ID.put("香港",21);
        PROVINCE_ID.put("TAIWAN",22);
        PROVINCE_ID.put("台湾",22);
        PROVINCE_ID.put("AOMEN",23);
        PROVINCE_ID.put("澳门",23);
        PROVINCE_ID.put("GUIZHOU",24);
        PROVINCE_ID.put("贵州",24);
        PROVINCE_ID.put("QINGHAI",25);
        PROVINCE_ID.put("青海",25);
        PROVINCE_ID.put("GANSU",26);
        PROVINCE_ID.put("甘肃",26);
        PROVINCE_ID.put("NINGXIA",27);
        PROVINCE_ID.put("宁夏",27);
        PROVINCE_ID.put("XIZANG",28);
        PROVINCE_ID.put("西藏",28);
        PROVINCE_ID.put("XINJIANG",29);
        PROVINCE_ID.put("新疆",29);
        PROVINCE_ID.put("NEIMENGGU",30);
        PROVINCE_ID.put("内蒙古",30);
        PROVINCE_ID.put("SHANDONG",31);
        PROVINCE_ID.put("山东",31);
        PROVINCE_ID.put("LIAONING",32);
        PROVINCE_ID.put("辽宁",32);
        PROVINCE_ID.put("JILIN",33);
        PROVINCE_ID.put("吉林",33);
        PROVINCE_ID.put("HEILONGJIANG",34);
        PROVINCE_ID.put("黑龙江",34);
    }
}
