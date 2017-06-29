package com.codelab.model;

import com...ad.predict.data.HistoricalDataCorpus;
import com...ad.predict.thrift.model.AdData;
import com...ad.predict.thrift.model.ContextData;
import com...ad.predict.thrift.model.UserData;
import com...ad.predict.util.TypeCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhujian on 14-11-10.
 */
public class RandomModel extends Model {
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomModel.class);
    private static Random random = new Random();
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
        /*
        Double value = Math.abs(random.nextGaussian());
        Double value_2 = value > 1.0 ? 1 / value : value * value;
        value = Math.abs(random.nextGaussian());
        Double value_3 = value > 1.0 ? 1 / value : value * value;
        return value_2 * value_3 / 10;
        */
        return random.nextDouble();
    }

    @Override
    public Double score(Set<String> featureValues, String expId, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean media) {
        return generateDefaultScore();
    }

    @Override
    public Double score(Map<String, Double> featureMap, String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, TypeCollection.Calculation featureType, Boolean optimalFlag) {
        return generateDefaultScore();
    }

    @Override
    public Float getWeight(String key) {
        return 0f;
    }
}
