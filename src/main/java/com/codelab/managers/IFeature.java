package com.codelab.managers;


import com.codelab.util.FeatureCollector;

/**
 * Created by liyongbao on 15-8-17.
 */
public interface IFeature {
    public FeatureCollector collect(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, FeatureCollector collector);
    public void get(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, Map<String, Double> featureMap);
    public Double getFeatureWeight(Model model, UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData);
}
