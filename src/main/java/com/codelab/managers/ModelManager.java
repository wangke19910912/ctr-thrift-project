package com.codelab.managers;

import com.codelab.common.utils.StringUtil;
import com.codelab.model.Model;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by liyongbao on 15-8-17.
 */
public class ModelManager {
    // 统一管理实验ID对应的模型，通过xml配置文件来加载
    public Map<String, Map<String, Integer>> expMap;
    public Map<Integer, List<Model>> modelMap;

    // selection part model, only 1 model supported present
    public Map<String, Map<String, Integer>> selectionExpModel;
    public Map<Integer, List<Model>>  selectionModelMap;
    private static final Logger LOGGER = LoggerFactory.getLogger(ModelManager.class);
    private static ModelManager modelManager = null;
    public double sampling_rate_1 = 1.0;
    public double sampling_rate_2 = 1.0;
    public double sampling_rate_3 = 1.0;

    private ModelManager(){}
    public static ModelManager getInstance(){
        if(modelManager == null)
            modelManager = new ModelManager();
        return modelManager;
    }
    /**
     * @param configPath 数据文件格式
     * <?xml version=”1.0” encoding=”UTF-8”?>
     * <models>
     * <appId>xxx</appId>
     * <layerId>xxx</layerId>
     * <model>
     *  <expId>xxx</expId>
     *  <class>className</class>
     *  <path>model/xxx-model</path>
     * </model>
     * </models>
     * @return 解析scorer配置文件成功返回true，否则返回false
     */

    public boolean init(String configData){
        expMap = new ConcurrentHashMap<String, Map<String, Integer>>();
        modelMap = new ConcurrentHashMap<Integer, List<Model>>();
        selectionExpModel = new ConcurrentHashMap<String, Map<String, Integer>>();
        selectionModelMap = new ConcurrentHashMap<Integer, List<Model>>();

        try {
            SAXReader reader = new SAXReader();
            //Document document = reader.read(FeatureManager.class.getResourceAsStream(configPath));
            if (configData.isEmpty()) {
                LOGGER.error("config data is empty");
                return false;
            }
            Document document = reader.read(new InputSource(new StringReader(configData)));
            Element rootElement = document.getRootElement();

            for(Element element : (List<Element>)rootElement.elements()) {
                String expId = element.attribute("id").getValue();
                for (Element sub_element : (List<Element>) element.elements()) {
                    if (sub_element.getName().equals("models")) {
                        for (Element sub_2_element : (List<Element>) sub_element.elements()) {
                            if (sub_2_element.getName().equals("scoring_models")) {
                                Map<String, Integer> models = new HashMap<String, Integer>();
                                for (Element child : (List<Element>) sub_2_element.elements("model")) {
                                    String billing_type = child.elementTextTrim("billing_type");

                                    String model_1_class = child.elementTextTrim("class_1");
                                    String cali_table_path_1 = child.elementTextTrim("cali_table_path_1");
                                    String model_path_1 = child.elementTextTrim("model_path_1");
                                    String value = child.elementTextTrim("sampling_rate_path_1");
                                    if (!StringUtils.isEmpty(value)) {
                                        sampling_rate_1 = Double.parseDouble(value);
                                    }
                                    LOGGER.debug("sampling_rate_1 is: {}", sampling_rate_1);

                                    String model_2_class = child.elementTextTrim("class_2");
                                    String cali_table_path_2 = child.elementTextTrim("cali_table_path_2");
                                    String model_path_2 = child.elementTextTrim("model_path_2");
                                    value = child.elementTextTrim("sampling_rate_path_2");
                                    if (!StringUtils.isEmpty(value)) {
                                        sampling_rate_2 = Double.parseDouble(value);
                                    }
                                    LOGGER.debug("sampling_rate_2 is: {}", sampling_rate_2);

                                    String model_3_class = child.elementTextTrim("class_3");
                                    String cali_table_path_3 = sub_element.elementTextTrim("cali_table_path_3");
                                    String model_path_3 = child.elementTextTrim("model_path_3");
                                    value = child.elementTextTrim("sampling_rate_path_3");
                                    if (!StringUtils.isEmpty(value)) {
                                        sampling_rate_3 = Double.parseDouble(value);
                                    }
                                    LOGGER.debug("sampling_rate_3 is: {}", sampling_rate_3);

                                    Integer key = (model_1_class + "," + model_path_1 + "," + cali_table_path_1 + ";" + model_2_class + "," + model_path_2 + "," + cali_table_path_2 + ";" + model_3_class + "," + model_path_3 + "," + cali_table_path_3).hashCode();

                                    if (modelMap.containsKey(key)) {
                                        for (String type : billing_type.split(":")) {
                                            models.put(type, key);
                                        }
                                        LOGGER.debug("model exist {}, billingTypes {}", key, billing_type);
                                        continue;
                                    }

                                    List<Model> model_list = new ArrayList<Model>();
                                    Model model_1 = (Model) Class.forName(model_1_class).newInstance();
                                    if (model_1.init(model_path_1, cali_table_path_1, sampling_rate_1, model_1_class)) {
                                        model_list.add(model_1);
                                        LOGGER.info("expId {}, model1 load success", expId);
                                    } else {
                                        LOGGER.error("expId {}, model1 load failed", expId);
                                        return false;
                                    }

                                    Model model_2 = null;
                                    if (!StringUtil.isEmpty(model_2_class)) {
                                        model_2 = (Model) Class.forName(model_2_class).newInstance();
                                        if (model_2.init(model_path_2, cali_table_path_2, sampling_rate_2,model_2_class)) {
                                            model_list.add(model_2);
                                            LOGGER.info("expId {}, model2 load success", expId);
                                        } else {
                                            LOGGER.error("expId {}, model2 load failed", expId);
                                            return false;
                                        }
                                    }

                                    Model model_3 = null;
                                    if (!StringUtil.isEmpty(model_3_class)) {
                                        model_3 = (Model) Class.forName(model_3_class).newInstance();
                                        if (model_3.init(model_path_3, cali_table_path_3, sampling_rate_3,model_3_class)) {
                                            model_list.add(model_3);
                                            LOGGER.info("expId {}, model3 load success", expId);
                                        } else {
                                            LOGGER.error("expId {}, model3 load failed", expId);
                                            return false;
                                        }
                                    }
                                    LOGGER.debug("model_list type init success! {}", model_list);
                                    modelMap.put(key, model_list);
                                    for (String type : billing_type.split(":")) {
                                        models.put(type, key);
                                        LOGGER.debug("add new model billingType {}, model {}", type, key);
                                    }
                                }
                                expMap.put(expId, models);
                            } else if (sub_2_element.getName().equals("selection_models")) {
                                Map<String, Integer> models = new HashMap<String, Integer>();
                                for (Element child : (List<Element>) sub_2_element.elements("model")) {
                                    String billing_type = child.elementTextTrim("billing_type");
                                    String model_1_class = child.elementTextTrim("class_1");
                                    String cali_table_path_1 = child.elementTextTrim("cali_table_path_1");
                                    String model_path_1 = child.elementTextTrim("model_path_1");
                                    String value = child.elementTextTrim("sampling_rate_path_1");
                                    if (!StringUtils.isEmpty(value)) {
                                        sampling_rate_1 = Double.parseDouble(value);
                                    }
                                    LOGGER.debug("sampling_rate_1 is: {}", sampling_rate_1);

                                    Integer key = (model_1_class + "," + model_path_1 + "," + cali_table_path_1).hashCode();

                                    if (selectionModelMap.containsKey(key)) {
                                        for (String type : billing_type.split(":")) {
                                            models.put(type, key);
                                        }
                                        LOGGER.debug("model exist {}, billingTypes {}", key, billing_type);
                                        continue;
                                    }

                                    List<Model> model_list = new ArrayList<Model>();
                                    Model model_1 = (Model) Class.forName(model_1_class).newInstance();
                                    if (model_1.init(model_path_1, cali_table_path_1, sampling_rate_1,model_1_class)) {
                                        model_list.add(model_1);
                                        LOGGER.info("expId {}, model1 load success", expId);
                                    } else {
                                        LOGGER.error("expId {}, model1 load failed", expId);
                                        return false;
                                    }

                                    LOGGER.debug("model_list type init success! {}", model_list);
                                    selectionModelMap.put(key, model_list);
                                    for (String type : billing_type.split(":")) {
                                        models.put(type, key);
                                        LOGGER.debug("add new model billingType {}, model {}", type, key);
                                    }
                                }
                                selectionExpModel.put(expId, models);
                            } else {
                                LOGGER.error("not found selection or scoring models {}", expId);
                                return false;
                            }
                        }
                    }
                }
                LOGGER.debug("expId {}, modelMap {}", expId,  modelMap);
                LOGGER.debug("expId {}, expMap {}", expId,  expMap);
                LOGGER.debug("expId {}, selectionModelMap {}", expId,  selectionModelMap);
                LOGGER.debug("expId {}, selectionExpModel {}", expId,  selectionExpModel);
            }
            if (expMap.isEmpty()) {
                LOGGER.error("model map empty, no model found");
                return false;
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("parse model config error: {}", e);
            return false;
        }
    }

    public List<Double> scoreSelection(String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, Boolean calcFlag, Boolean optimalFlag){
        return score(expId, userData, adData, historicalDataCorpus, contextData, lookBackWindow, mediaFlag, calcFlag, TypeCollection.Calculation.SELECTION, optimalFlag);
    }
    public List<Double> scoreModel(String expId,  UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, Boolean calcFlag, Boolean optimalFlag){
        return score(expId, userData, adData, historicalDataCorpus, contextData, lookBackWindow, mediaFlag, calcFlag, TypeCollection.Calculation.SCORING, optimalFlag);
    }

    public List<Double> score(String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, Boolean calcFlag, TypeCollection.Calculation calculation, Boolean optimalFlag){
        double score;
        List<Double> score_list = new ArrayList<Double>();
        Map<String, Integer> models = (calculation == TypeCollection.Calculation.SELECTION) ? selectionExpModel.get(expId) : expMap.get(expId);
        LOGGER.debug("models is: {}", models);
        if (models == null) {
            LOGGER.warn("get model error for id: {}, return null arrayList", expId);
            return score_list;
        }

        String billingTypeStr = String.valueOf(adData.getBillingType());
        LOGGER.debug("model billing_type: {}", billingTypeStr);

        if (models.containsKey(billingTypeStr)) {
            Integer billingTypeKey = models.get(billingTypeStr);
            LOGGER.debug("models value: {}", billingTypeKey);
            Map<Integer, List<Model>> tempModelMap = (calculation == TypeCollection.Calculation.SELECTION) ? selectionModelMap : modelMap;

            if (tempModelMap.containsKey(billingTypeKey)) {
                Map<String, Double> featureMap = new HashMap<String, Double>();
                List<Model> model_list = tempModelMap.get(billingTypeKey);
                LOGGER.debug("model_list is {}", model_list);
                if (model_list.size() > 1) {
                    // if model size more than one, no direct score
                    optimalFlag = false;
                }
                if (!optimalFlag) {
                    Long time = System.currentTimeMillis();
                    if (calculation == TypeCollection.Calculation.SCORING) {
                        featureMap = FeatureManager.getInstance().getScoringFeatures(contextData, userData, adData, historicalDataCorpus, expId);
                    }
                    else {
                        featureMap = FeatureManager.getInstance().getSelectionFeatures(contextData, userData, adData, historicalDataCorpus, expId);
                    }
                    PerfCounter.count("online_CTR_FeatureExtractionTime", 1L, System.currentTimeMillis() - time);
                    PerfCounter.count("online_CTR_FeatureExtractionTime_expId" + expId, 1L, System.currentTimeMillis() - time);
                }
                if (calcFlag) {
                    for (Model model : model_list) {
                        LOGGER.debug("model elem is: {}",model);
                        if (model != null) {
                            LOGGER.debug("begin calc ctr process...");
                            score = model.score(featureMap, expId, userData, adData, historicalDataCorpus, contextData, lookBackWindow, mediaFlag, calculation, optimalFlag);
                            LOGGER.debug("parallel model level, score: {}", score);
                            score_list.add(score);
                        }
                    }
                    LOGGER.debug("score_list result is: {}", score_list);
                } else {
                    score = 1.0;
                    for (Model model : model_list) {
                        if (model != null) {
                            score *= model.score(featureMap, expId, userData, adData, historicalDataCorpus, contextData, lookBackWindow, mediaFlag, calculation, optimalFlag);
                            LOGGER.debug("thread model level, score: {}", score);
                        }
                    }
                    score_list.add(score);
                }
            }
        }
        else {
            PerfCounter.count("Online_PTR_model_billingType_unknown_" + expId, 1L);
        }
        return score_list;
    }
}
