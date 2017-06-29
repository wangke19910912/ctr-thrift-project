package com.codelab.model;

import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.thrift.model.AdData;
import com.xiaomi.miui.ad.predict.thrift.model.ContextData;
import com.xiaomi.miui.ad.predict.thrift.model.HistoricalData;
import com.xiaomi.miui.ad.predict.thrift.model.UserData;
import com.xiaomi.miui.ad.predict.util.TypeCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhujian on 14-11-10.
 */
public class MAModel extends Model {
    private static final Logger LOGGER = LoggerFactory.getLogger(MAModel.class);
    //private Double defaultCTR = 0.000001;
    //private Double defaultCVR = 0.000001;
    //private DataManager dataManager = DataManager.getInstance();

    @Override
    protected boolean onModelChanged() {
        return true;
    }

    @Override
    protected boolean onCaliTableChanged() {
        return true;
    }


    @Override
    public boolean init(String modelPath, String caliTablePath, Double sampling_rate, String modelName) {
        //this.defaultCTR = defaultCTR;
        //this.defaultCVR = defaultCVR;
        return true;
    }

    @Override
    public TreeMap<Float, Float> readCaliTable(Configuration conf, String caliTablePath) {
        TreeMap<Float, Float> caliTable = new TreeMap<Float, Float>();
        return caliTable;
    }

    public Map<Integer, Float> readModel(Configuration conf, String modelPath) {
        Map<Integer, Float> model = new HashMap<Integer, Float>();
        return model;
    }

    private Double generateDefaultScore() {
        Random random = new Random();
        Double value = Math.abs(random.nextGaussian());
        Double value_2 = value > 1.0 ? 1 / value : value * value;
        value = Math.abs(random.nextGaussian());
        Double value_3 = value > 1.0 ? 1 / value : value * value;
        return value_2 * value_3 / 10;
    }

    @Override
    public Double score(Set<String> featureValues, String expId, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean media) {

        String mediaId = contextData.getParameters().get("MEDIA_ID");
        String key =  (String.valueOf(adData.getId()) + "*" +  (media ? (mediaId + "*0") : contextData.getTagId()) + "*latest*" + lookBackWindow) ;

        Boolean onlineAd = Boolean.parseBoolean(contextData.getParameters().get("ENABLE_ONLINE_ADHISTORY"));
        HistoricalData historicalData = historicalDataCorpus.getData(key, onlineAd);
        switch (adData.getBillingType()) {
            case cpc:
                if (historicalData == null) {
                    return generateDefaultScore();
                }
                return historicalData.getCtr();
            case cpd:
            default:
                if (historicalData == null) {
                    return generateDefaultScore() / 10;
                }
                return historicalData.getCvr();
        }
    }

    @Override
    public Double score(Map<String, Double> featureMap, String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, TypeCollection.Calculation featureType, Boolean optimalFlag) {
        //DateTime dateTime = new DateTime(contextData.getTimestamp()).minusHours(1);
        String mediaId = contextData.getParameters().get("MEDIA_ID");
        String key =  (String.valueOf(adData.getId()) + "*" +  (mediaFlag ? (mediaId + "*0") : contextData.getTagId()) + "*latest*" + lookBackWindow) ;


        Boolean onlineAd = Boolean.parseBoolean(contextData.getParameters().get("ENABLE_ONLINE_ADHISTORY"));
        HistoricalData historicalData = historicalDataCorpus.getData(key, onlineAd);
        LOGGER.debug("MAModel key {}, histData {}", key, historicalData);
        switch (adData.getBillingType()) {
            case cpc:
                if (historicalData == null) {
                    return generateDefaultScore();
                }
                return historicalData.getCtr();
            case cpd:
            default:
                if (historicalData == null) {
                    return generateDefaultScore() / 10;
                }
                return historicalData.getCvr();
        }
    }

    @Override
    public Float getWeight(String key) {
        return 0f;
    }
}
