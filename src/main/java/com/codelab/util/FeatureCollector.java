package com.codelab.util;

import com...ad.predict.model.Model;

import java.util.Map;

/**
 * Created by mengqingdi on 17-4-17.
 */
public class FeatureCollector {

    public boolean isDirectScore;
    public double score;
    public Map<String, Double> featureMap;
    public Model model;

    public FeatureCollector(boolean isDirectScore, Map<String, Double> featureMap, Model model) {
        this.isDirectScore = isDirectScore;
        this.featureMap = featureMap;
        this.model = model;
        this.score = 0.0;
    }

    public void add(String feature) {
        if (this.isDirectScore) {
            score += model.getWeight(feature);
        } else {
            featureMap.put(feature, 1.0);
        }
    }

    public void add(String feature, Double weight) {
        if (this.isDirectScore) {
            score += model.getWeight(feature) * weight;
        } else {
            featureMap.put(feature, weight);
        }
    }

    public Map<String, Double> getFeatureMap() {
        return this.featureMap;
    }

    public Double getScore() {
        return this.score;
    }
}
