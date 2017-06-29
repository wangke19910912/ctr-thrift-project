package com.codelab;

import java.util.List;
import java.util.Map;

/**
 * should generate from  thrift
 */
public interface PredictService {
    Map<Long, Double> getCtrs(
            final Map<String, List<String>> clientInfo,
            final List<Long> retrieveAds,
            final String expId);


}
