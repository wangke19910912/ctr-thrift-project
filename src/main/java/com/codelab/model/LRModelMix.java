package com.codelab.model;

import com.meidusa.amoeba.util.StringUtil;
import com.xiaomi.common.perfcounter.PerfCounter;
import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.feature.FeatureManager;
import com.xiaomi.miui.ad.predict.feature.IFeature;
import com.xiaomi.miui.ad.predict.thrift.model.AdData;
import com.xiaomi.miui.ad.predict.thrift.model.ContextData;
import com.xiaomi.miui.ad.predict.thrift.model.UserData;
import com.xiaomi.miui.ad.predict.util.ConstantManager;
import com.xiaomi.miui.ad.predict.util.TypeCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by mengqingdi on 17-4-6.
 */
public class LRModelMix extends Model {

    private static final Logger LOGGER = LoggerFactory.getLogger(LRModelMix.class);
    private Map<Integer, Float> modelByHashCode = null;
    private Map<String, Float> modelByRaw = null;
    private TreeMap<Float, Float> caliTable = null;
    private Double minWeight = 0.0000001;
    private Double defaultCTR = 0.0000001;
    private ConstantManager common = ConstantManager.getInstance();

    @Override
    protected boolean onModelChanged() {
        Configuration conf =  new Configuration();
        ArrayList<Map> models = readModel(conf, modelPath);
        if (models.size() == 2) {
            modelByHashCode = models.get(0);
            modelByRaw = models.get(1);
            LOGGER.info("update model: " + modelPath + " succeed");
            return true;
        }
        return false;
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
    public boolean init(String modelPath, String caliTablePath, Double samplingRate, String modelName) {
        this.modelPath = modelPath;
        this.caliTablePath = caliTablePath;
        this.samplingRate = samplingRate;
        LOGGER.debug("sampling rate is : {}",  this.samplingRate);
        LOGGER.debug("model path is {}", modelPath);
        Configuration conf =  new Configuration();
        ArrayList<Map> models = readModel(conf, modelPath);
        if (models.size() != 2) {
            return false;
        }
        modelByHashCode = models.get(0);
        modelByRaw = models.get(1);
        if (modelByHashCode == null || modelByHashCode.isEmpty()) {
            return false;
        }
        if (modelByRaw == null) {
            return false;
        }
        if (!StringUtil.isEmpty(caliTablePath)) {
            caliTable = readCaliTable(conf, caliTablePath);
            LOGGER.debug("cali table size : {}", caliTable.size());
            if (caliTable == null || caliTable.isEmpty())
                return false;
        }
        start();
        return true;
    }

    @Override
    protected TreeMap<Float, Float> readCaliTable(Configuration conf, String caliTablePath) {
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

    public ArrayList<Map> readModel(Configuration conf, String modelPath) {
        ArrayList<Map> ret = new ArrayList<Map>();
        Map<Integer, Float> modelByHashCode = null;
        Map<String, Float> modelByRaw = null;
        Map<Integer, Float> hash_model = null;
        try {
            LOGGER.debug("fs.defaultFS:{}", conf.get("fs.defaultFS"));
            LOGGER.debug("fs.default.name:{}", conf.get("fs.default.name"));
            LOGGER.debug("dfs.nameservices:{}", conf.get("dfs.nameservices"));
            LOGGER.debug("dfs.namenode.http-address.lgprc-xiaomi.host0 : {}", conf.get("dfs.namenode.http-address.lgprc-xiaomi.host0"));
            LOGGER.debug("dfs.block.access.token.enable : {}" ,conf.get("dfs.block.access.token.enable"));
            FileSystem fs = FileSystem.get(conf);
            LOGGER.debug("fs.getHomeDirectory :{}", fs.getHomeDirectory());
            LOGGER.debug("fs.getUri :{}", fs.getUri());

            Path inFile = new Path(modelPath);
            LOGGER.debug("inFile.toUri().getPath():{}", inFile.toUri().getPath());
            LOGGER.debug("open begin");
            FSDataInputStream fin = fs.open(inFile);
            LOGGER.debug("fs open success");
            BufferedReader input = new BufferedReader(new InputStreamReader(fin));
            LOGGER.debug("new BufferedReader");
            String line;
            modelByHashCode = new HashMap<Integer, Float>();
            modelByRaw = new HashMap<String, Float>();
            while ((line = input.readLine()) != null) {
                String[] items = line.split("\t");
                if (items.length < 2) {
                    continue;
                }
                String fea = items[0];
                String weight = items[1];
                if(Math.abs(Float.valueOf(weight)) > minWeight) {
                    Integer feahash = fea.hashCode();
                    if (!modelByHashCode.containsKey(feahash)) {
                        modelByHashCode.put(feahash, Float.valueOf(weight));
                    } else {
                        if (!modelByRaw.containsKey(fea)) {
                            modelByRaw.put(fea, Float.valueOf(weight));
                        }
                    }
                }
            }

            ret.add(modelByHashCode);
            ret.add(modelByRaw);

            input.close();
            fin.close();
        } catch (Exception ex) {
            LOGGER.error("Read model file failed : {}", ex.getMessage());
        }
        return ret;
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
    public Double score(Set<String> featureValues, String expId, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag) {
        double sum = 0.0f;
        double score = 0.0f;
        if (featureValues.isEmpty()) {
            LOGGER.debug("feature values empty");
            return defaultCTR;
        }

        if (modelByRaw.containsKey(common.getData(expId).getKey("BIAS_FEATURE"))) {
            sum += modelByRaw.get(common.getData(expId).getKey("BIAS_FEATURE"));
        } else if (modelByHashCode.containsKey(common.getData(expId).getKey("BIAS_FEATURE").hashCode())) {
            sum += modelByHashCode.get(common.getData(expId).getKey("BIAS_FEATURE").hashCode());
        }

        int featureHitCount = 0;
        for (String fea : featureValues) {
            Integer feaHash = fea.hashCode();
            if (modelByRaw.containsKey(fea)) {
                ++featureHitCount;
                sum += modelByRaw.get(fea);
            } else if (modelByHashCode.containsKey(feaHash)) {
                ++featureHitCount;
                sum += modelByHashCode.get(feaHash);
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
        }
        else if (samplingRate > 0 && samplingRate < 1) {
            score = rawScore / (rawScore + (1 - rawScore)/samplingRate);
            LOGGER.debug("generate cali result {}, raw {}，samplingRate {}", score, rawScore, samplingRate);
            return score;
        }
        else {
            LOGGER.debug("no cali, raw score {}", score);
            return rawScore;
        }
    }

    @Override
    public Double score(Map<String, Double> featureMap, String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, TypeCollection.Calculation featureType, Boolean optimalFlag) {
        double sum = 0.0;
        double score = 0.0;

        if (optimalFlag) {
            List<IFeature> features = FeatureManager.getInstance().getFeatureMap(featureType).get(expId);
            if (features.isEmpty()){
                LOGGER.debug("feature values empty");
                return this.defaultCTR;
            }

            String fea = contextData.getParameters().get("BIAS_FEATURE");
            Integer feaHash = fea.hashCode();

            if (modelByRaw.containsKey(fea)) {
                sum += modelByRaw.get(fea);
            } else if (modelByHashCode.containsKey(feaHash)) {
                sum += modelByHashCode.get(feaHash);
            }

            for(IFeature feature : features) {
                sum += feature.getFeatureWeight(this, userData, adData, contextData, historicalDataCorpus);
            }
        } else {
            if (featureMap.isEmpty()){
                LOGGER.debug("feature values empty");
                return this.defaultCTR;
            }

            String fea = contextData.getParameters().get("BIAS_FEATURE");
            Integer feaHash = fea.hashCode();

            if (modelByRaw.containsKey(fea)) {
                sum += modelByRaw.get(fea);
            } else if (modelByHashCode.containsKey(feaHash)) {
                sum += modelByHashCode.get(feaHash);
            }

            for(Map.Entry<String, Double> entry : featureMap.entrySet()) {
                fea = entry.getKey();
                feaHash = fea.hashCode();
                if (modelByRaw.containsKey(fea)) {
                    sum += modelByRaw.get(fea) * entry.getValue();
                } else if (modelByHashCode.containsKey(feaHash)) {
                    sum += modelByHashCode.get(feaHash) * entry.getValue();
                }
            }
        }

        Double rawScore = (double) (1.0 / (1 + Math.exp(-sum)));
        if (caliTable != null) {
            score = getCaliedScore(rawScore);
            LOGGER.debug("generate cali result {}, raw {}", score, rawScore);
            return score;
        }
        else if (samplingRate > 0 && samplingRate < 1) {
            score = rawScore / (rawScore + (1 - rawScore)/samplingRate);
            LOGGER.debug("generate cali result {}, raw {}，samplingRate {}", score, rawScore, samplingRate);
            return score;
        }
        else {
            LOGGER.debug("no cali, raw score {}", rawScore);
            return rawScore;
        }
    }

    @Override
    public Float getWeight(String key) {
        if (modelByRaw != null) {
            Float weight = modelByRaw.get(key);
            if (weight == null) {
                if (modelByHashCode != null) {
                    weight = modelByHashCode.get(key.hashCode());
                    if (weight == null) {
                        return 0f;
                    }
                    return weight;
                }
                return 0f;
            }
            return weight;
        }
        return 0f;
    }
}
