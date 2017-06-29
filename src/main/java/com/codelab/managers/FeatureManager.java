package com.codelab.managers;


import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.*;
public class FeatureManager {


    // 统一管理所有的特征和实验
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureManager.class);
    private static FeatureManager featureManager = null;
    private Map<String,List<IFeature>> scoringMap;
    private Map<String,List<IFeature>> selectionMap;
    private DataManager dataManager = DataManager.getInstance();


    private FeatureManager(){}
    public static FeatureManager getInstance(){
        if(featureManager == null)
            featureManager = new FeatureManager();
        return featureManager;
    }


    /*for offline training code use, not use any more*/
    public boolean init(String configData, String expId){
        scoringMap = new HashMap<String, List<IFeature>>();
        selectionMap = new HashMap<String, List<IFeature>>();
        try {
            SAXReader reader = new SAXReader();
            //Document document = reader.read(FeatureManager.class.getResourceAsStream(configPath));
            if (configData.isEmpty()) {
                LOGGER.error("config data is empty");
                return false;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));
            Element rootElement = document.getRootElement();

            for(Element element : (List<Element>)rootElement.elements()){
                String pexpId = element.attribute("id").getValue();
                if (expId.equals(pexpId)){// && pmediaId.equals(mediaId)) {
                    for (Element sub_element : (List<Element>) element.elements()) {

                        if (sub_element.getName().equals("features")) {
                            for (Element sub_2_element: (List<Element>)sub_element.elements()) {
                                List<IFeature> features = new ArrayList<IFeature>();
                                if (sub_2_element.getName().equals("selection_features")) {
                                    for (Element child : (List<Element>) sub_2_element.elements("feature")) {
                                        String className = child.elementTextTrim("class");
                                        IFeature feature = (IFeature) Class.forName(className).newInstance();
                                        features.add(feature);
                                    }
                                    selectionMap.put(expId, features);
                                }
                                else if (sub_2_element.getName().equals("scoring_features")) {
                                    for (Element child : (List<Element>) sub_2_element.elements("feature")) {
                                        String className = child.elementTextTrim("class");
                                        IFeature feature = (IFeature) Class.forName(className).newInstance();
                                        features.add(feature);
                                    }
                                    scoringMap.put(expId, features);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("parse feature config error:{}", e.getMessage());
            return false;
        }
        return true;
    }

    public boolean init(String configData){
        scoringMap = new HashMap<String, List<IFeature>>();
        selectionMap = new HashMap<String, List<IFeature>>();
        try {
            SAXReader reader = new SAXReader();
            //Document document = reader.read(FeatureManager.class.getResourceAsStream(configPath));
            if (configData.isEmpty()) {
                LOGGER.error("config data is empty");
                return false;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));
            Element rootElement = document.getRootElement();

            for(Element element : (List<Element>)rootElement.elements()){
                String expId = element.attribute("id").getValue();
                for (Element sub_element: (List<Element>)element.elements()) {
                    if (sub_element.getName().equals("features")) {
                        for (Element sub_2_element: (List<Element>)sub_element.elements()) {
                            List<IFeature> features = new ArrayList<IFeature>();
                            if (sub_2_element.getName().equals("selection_features")) {
                                for (Element child : (List<Element>) sub_2_element.elements("feature")) {
                                    String className = child.elementTextTrim("class");
                                    IFeature feature = (IFeature) Class.forName(className).newInstance();
                                    features.add(feature);
                                }
                                selectionMap.put(expId, features);
                            }
                            else if (sub_2_element.getName().equals("scoring_features")) {
                                for (Element child : (List<Element>) sub_2_element.elements("feature")) {
                                    String className = child.elementTextTrim("class");
                                    IFeature feature = (IFeature) Class.forName(className).newInstance();
                                    features.add(feature);
                                }
                                scoringMap.put(expId, features);
                            }
                            else {
                                LOGGER.error("not found selection or scoring features {}", expId);
                                return false;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("parse feature config error:{}", e.getMessage());
            return false;
        }
        return true;
    }



    public Map<String,Double> getSelectionFeatures(ContextData contextData, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, String expId) {
        LOGGER.debug("selection features");
        Map<String,Double> featureMap = new HashMap<String, Double>();
        return get(contextData, userData, adData, historicalDataCorpus, expId, featureMap, TypeCollection.Calculation.SELECTION);
    }


    public Map<String,Double> getScoringFeatures(ContextData contextData, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, String expId) {
        LOGGER.debug("scoring features");
        Map<String,Double> featureMap = new HashMap<String, Double>();
        return get(contextData, userData, adData, historicalDataCorpus, expId, featureMap, TypeCollection.Calculation.SCORING);

    }

    /*for offline_training using, actually this project not used any more, deprecated*/
    public Map<String,Double> get(ContextData contextData, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, String expId) {
        return getScoringFeatures(contextData, userData, adData, historicalDataCorpus, expId);
    }


    public Map<String,Double> get(ContextData contextData, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, String expId, Map<String, Double> featureMap, TypeCollection.Calculation featureType) {
        featureMap.clear();
        if (expId.isEmpty()) {
            return featureMap;
        }
        if (adData == null) {
            LOGGER.debug("adData null : {}", adData.getId());
            return featureMap;
        }
        List<IFeature> features = (featureType == TypeCollection.Calculation.SELECTION ? selectionMap.get(expId) : scoringMap.get(expId));
        for (IFeature feature : features) {
            Long time = System.currentTimeMillis();
            feature.get(userData, adData, contextData, historicalDataCorpus, featureMap);
            LOGGER.debug("feature name : {}", feature.getClass().getName());
        }
        LOGGER.debug("userData adBehavior: {}", userData.getAdBehavior());
        LOGGER.debug("userData videoBehavior: {}", userData.getVideoBehavior());
        LOGGER.debug("all features : {}", featureMap);
        LOGGER.debug("all features key : {}", featureMap.keySet());
        return featureMap;
    }

    public Map<String, List<IFeature>> getFeatureMap(TypeCollection.Calculation featureType) {
        if (featureType == TypeCollection.Calculation.SELECTION && selectionMap != null) {
            return selectionMap;
        }
        if (featureType == TypeCollection.Calculation.SCORING && scoringMap != null) {
            return scoringMap;
        }
        return Collections.EMPTY_MAP;
    }
}
