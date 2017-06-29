package com.codelab.model;

import com.meidusa.amoeba.util.StringUtil;
import com..common.perfcounter.PerfCounter;
import com...ad.predict.data.HistoricalDataCorpus;
import com...ad.predict.feature.FeatureManager;
import com...ad.predict.feature.IFeature;
import com...ad.predict.proxy.PegasusAccessor;
import com...ad.predict.thrift.model.AdData;
import com...ad.predict.thrift.model.ContextData;
import com...ad.predict.thrift.model.UserData;
import com...ad.predict.util.ConstantManager;
import com...ad.predict.util.TypeCollection;
import com...ad.predict.util.ZkAppConfigForStatic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by vincent on 2017/4/24.
 */
public class StreamLRModel extends Model{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamLRModel.class);

    private static final String modelName = "ModelName";
    private static final String tableName = "_adlabel";
    private static final int timeToUpdateModelInternel = 600;
    private static final Double defaultCTR = 0.0000001;

    private List<Runnable> tasks = new ArrayList<Runnable>();
    private TreeMap<Float, Float> caliTable = null;
    private ConstantManager common = ConstantManager.getInstance();
    private Map<Long, Float> feaValueMap = new ConcurrentHashMap<Long, Float>();
    private String mName = "";
    private String mVersion = "";

    private ZkAppConfigForStatic zkAppConfigStatic = ZkAppConfigForStatic.getInstance();


    @Override
    public boolean onModelChanged(){
        return true;
    }

    @Override
    protected boolean onCaliTableChanged() {
        Configuration conf =  new Configuration();
        TreeMap<Float, Float> caliTableNew = readCaliTable(conf, caliTablePath);
        if (caliTableNew != null && !caliTableNew.isEmpty()) {
            caliTable = caliTableNew;
            LOGGER.info("update cali table : " + caliTablePath + " succeed");
            return true;
        }
        return false;
    }

    @Override
    public boolean init(String modelPath, String caliTablePath, Double samplingRate, String mName) {
        this.modelPath = modelPath;
        this.caliTablePath = caliTablePath;
        this.samplingRate = samplingRate;
        this.mName = modelPath;

        //this.mName = zkAppConfigStatic.GetStringValue(mName, mName);

        LOGGER.info("model path is {},{},{}", modelPath, this.mName, mName);

        feaValueMap.put(0l, 0f);

        /*if(!updateModel()){
            LOGGER.error("updateModel failed! {},{}", modelPath, mName);
            return false;
        }*/

        if(feaValueMap == null || feaValueMap.isEmpty()){
            LOGGER.error("init failed! for feaValueMap!");
            return false;
        }

        if (!StringUtil.isEmpty(caliTablePath)) {
            Configuration conf =  new Configuration();
            caliTable = readCaliTable(conf, caliTablePath);
            LOGGER.debug("cali table size : {}", caliTable.size());
            if (caliTable == null || caliTable.isEmpty())
                return false;
        }

        tasks.add(new StreamLRModelUpdater());

        if (tasks.size() > 0) {
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(tasks.size());
            for (Runnable task : tasks) {
                scheduledExecutorService.scheduleAtFixedRate(task, 0, timeToUpdateModelInternel, TimeUnit.SECONDS);
            }
        }

        return true;
    }

    @Override
    public TreeMap<Float, Float> readCaliTable(Configuration conf, String caliTablePath) {
        TreeMap<Float, Float> caliTable = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(caliTablePath);
            FSDataInputStream fin = fs.open(inFile);
            BufferedReader input = new BufferedReader(new InputStreamReader(fin));
            String line;
            caliTable = new TreeMap<Float, Float>();
            while ((line = input.readLine()) != null) {
                String[] items = line.split("\t");
                if (items.length < 2) {
                    continue;
                }
                Float predictedScore = Float.parseFloat(items[0]);
                Float caliedScore = Float.parseFloat(items[1]);
                caliTable.put(predictedScore, caliedScore);
            }
            input.close();
            fin.close();
        } catch (Exception ex) {
            LOGGER.error("Read cali file failed : {}", ex.getMessage());
        }
        return caliTable;
    }

    private Double getCaliedScore(Double rawScore) {
        Double caliedScore = 0.0;
        Iterator iter = caliTable.entrySet().iterator();
        Double lastValue = 0.0;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry)iter.next();
            Double key = (Double)(entry.getKey());
            Double value = (Double)entry.getValue();
            lastValue = value;
            if (rawScore < key) {
                caliedScore = value;
                break;
            }
        }
        if (caliedScore == 0.0F) {
            caliedScore = lastValue;
        }
        return caliedScore;
    }

    @Override
    public Double score(Set<String> featureValues, String expId, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData,
                        String lookBackWindow, Boolean mediaFlag){
        double sum = 0.0f;
        double score = 0.0f;
        if (featureValues.isEmpty()) {
            LOGGER.debug("feature values empty");
            return defaultCTR;
        }

        sum += getWeight("W0");

        int featureHitCount = 0;
        for (String fea : featureValues) {
            score = getWeight(fea);
            if(score != 0.0){
                ++featureHitCount;
                sum += score;
            }
        }

        PerfCounter.setGaugeValue("YiDianNews_online_feature_hit_ratio_X1000_" + expId, featureHitCount * 1000 / featureValues.size());
        PerfCounter.setGaugeValue("YiDianNews_online_feature_hit_count_" + expId, featureHitCount);
        PerfCounter.setGaugeValue("YiDianNews_online_feature_count_" + expId, featureValues.size());


        Double rawScore = (double) (1.0 / (1 + Math.exp(-sum)));
        if (caliTable != null) {
            score = getCaliedScore(rawScore);
            LOGGER.debug("generate cali result {}, raw {}", score, rawScore);
            return score;
        }else if (samplingRate > 0 && samplingRate < 1) {
            score = rawScore / (rawScore + (1 - rawScore)/samplingRate);
            LOGGER.debug("generate cali result {}, raw {}，samplingRate {}", score, rawScore, samplingRate);
            return score;
        }else {
            LOGGER.debug("no cali, raw score {}", score);
            return rawScore;
        }
    }

    @Override
    public Double score(Map<String,Double> featureMap, String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus,
                        ContextData contextData, String lookBackWindow, Boolean mediaFlag, TypeCollection.Calculation featureType, Boolean optimalFlag) {
        double sum = 0.0;
        double score = 0.0;

        if (optimalFlag) {
            List<IFeature> features = FeatureManager.getInstance().getFeatureMap(featureType).get(expId);
            if (features.isEmpty()) {
                LOGGER.debug("feature values empty");
                return defaultCTR;
            }

            sum += getWeight("WO");
            for (IFeature feature : features) {
                sum += feature.getFeatureWeight(this, userData, adData, contextData, historicalDataCorpus);
            }
        } else {
            if (featureMap.isEmpty()) {
                LOGGER.debug("feature values empty");
                return this.defaultCTR;
            }

            sum += getWeight("WO");
            for (Map.Entry<String, Double> entry : featureMap.entrySet()) {
                sum += getWeight(entry.getKey())*entry.getValue();
            }
        }

        Double rawScore = (double) (1.0 / (1 + Math.exp(-sum)));
        if (caliTable != null) {
            score = getCaliedScore(rawScore);
            LOGGER.debug("generate cali result {}, raw {}", score, rawScore);
            return score;
        } else if (samplingRate > 0 && samplingRate < 1) {
            score = rawScore / (rawScore + (1 - rawScore) / samplingRate);
            LOGGER.debug("generate cali result {}, raw {}，samplingRate {}", score, rawScore, samplingRate);
            return score;
        } else {
            LOGGER.debug("no cali, raw score {}", rawScore);
            return rawScore;
        }
    }

    @Override
    public Float getWeight(String key) {
        float w = 0;
        try {
            long fkey = hash(key+ "#" + mName);
            PerfCounter.count("StreamLRModel_ALL" , 1L);
            if(feaValueMap.containsKey(fkey)){
                w = feaValueMap.get(fkey);
                PerfCounter.count("StreamLRModel_HIT" , 1L);
            }
        }catch (Exception e){
            LOGGER.error("getWeight exp {},{}",e, key);
        }finally {
            return w;
        }
    }

    private long hash(String str){
        long seed = 131; // 31 131 1313 13131 131313 etc..
        long hash = 0;

        for (int i = 0; i< str.length(); i++){
            hash = hash *seed + str.charAt(i);
        }

        return ((hash) & 0x7FFFFFFFFFFFFFFFL);
    }

    private boolean updateModel(){
        boolean updateMVersion = false;
        String oldVersion = mVersion;
        int oldSize = feaValueMap.size();

        Map<String,String> modelVersionRaw = new HashMap<String, String>();
        Map<String,String> featureRaw = new HashMap<String, String>();
        Map<Long,Float> modelValueNew = new ConcurrentHashMap<Long, Float>();
        try {
            modelVersionRaw = PegasusAccessor.getScan(tableName,modelName,"","", 30000);

            LOGGER.info("updateModel {},{},{}", tableName, modelName, modelVersionRaw);
            if(!modelVersionRaw.isEmpty()){
                for(Map.Entry<String,String> entry: modelVersionRaw.entrySet()){
                    String[] modelVersionArray = entry.getKey().split("#");

                    LOGGER.info("updateModel modelVersionArray {}", modelVersionArray);
                    if(modelVersionArray.length != 2){
                        LOGGER.error("Bad ModelVersion {}", entry.getKey());
                        continue;
                    }

                    LOGGER.info("updateModel modelVersionArray equal {},{}", modelVersionArray[0].equalsIgnoreCase(mName), mName);
                    if(modelVersionArray[0].equalsIgnoreCase(mName)){
                        if(StringUtil.isEmpty(mVersion) || Long.parseLong(mVersion) < Long.parseLong(modelVersionArray[1])){
                            mVersion = modelVersionArray[1];
                            updateMVersion = true;
                        }
                    }
                    LOGGER.info("updateModel modelVersionArray updateMVersion {}", updateMVersion);
                }

                if(updateMVersion){
                    featureRaw = PegasusAccessor.getScan(tableName,(mName+"#"+mVersion),"","",60000);
                    LOGGER.info("updateModel featureRaw size {}", featureRaw.size());

                    if(featureRaw.size() > 0){
                        for(Map.Entry<String,String> en: featureRaw.entrySet()){
                            modelValueNew.put(Long.parseLong(en.getKey()), Float.parseFloat(en.getValue()));
                        }
                        feaValueMap = modelValueNew;
                    }else{
                        LOGGER.info("updateModel StreamLRModelUpdater @Version {},{}",oldVersion, mVersion);
                        LOGGER.info("updateModel StreamLRModelUpdater @Size {},{}",oldSize,feaValueMap.size());
                        mVersion = oldVersion;
                        updateMVersion = false;
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Read model file failed : {}", ex.getMessage());
            updateMVersion = false;
        } finally {
            return updateMVersion;
        }
    }

    private class StreamLRModelUpdater implements Runnable{
        @Override
        public void run() {
            updateModel();
        }
    }
}
