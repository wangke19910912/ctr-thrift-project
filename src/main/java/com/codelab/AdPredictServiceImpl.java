package com.codelab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

public class AdPredictServiceImpl implements PredictService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdPredictServiceImpl.class);
    private static AdPredict AdPredictService = AdPredict.getInstance();

    public AdPredictServiceImpl() {
        super();
    }

    @Override
    public Map<Long, Double> getCtrs(
            final Map<String, List<String>> clientInfo,
            final List<Long> retrieveAds,
            final String expId) {


        return null;
    }


}
