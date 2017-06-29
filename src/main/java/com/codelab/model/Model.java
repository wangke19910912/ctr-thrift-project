package com.codelab.model;

import com.meidusa.amoeba.util.StringUtil;
import com..common.perfcounter.PerfCounter;
import com...ad.predict.data.HistoricalDataCorpus;
import com...ad.predict.thrift.model.AdData;
import com...ad.predict.thrift.model.ContextData;
import com...ad.predict.thrift.model.UserData;
import com...ad.predict.util.CommonConstant;
import com...ad.predict.util.TypeCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhujian on 14-11-7.
 */
public abstract class Model {
    private static final Logger LOGGER = LoggerFactory.getLogger(Model.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private long modelLastModified;
    private long caliTableLastModified;
    protected String modelPath;  // 模型文件路径
    protected String caliTablePath;  // 模型文件路径
    protected double samplingRate = 1.0;
    //protected long period = 5;  // 周期更新时间间隔

    /**
     * 定期检测模型文件是否发生变化，若发生变化则进行模型更新
     */
    protected void start() {
        final Runnable task = new Runnable() {
            public void run() {
                try {
                    Configuration conf = new Configuration();
                    FileSystem fs = FileSystem.get(conf);
                    FileStatus fileStatus = fs.getFileStatus(new Path(modelPath));
                    long timeStamp = fileStatus.getModificationTime();
                    if (modelLastModified != timeStamp) {
                        long start = System.currentTimeMillis();
                        if (!onModelChanged()) {
                            LOGGER.warn("update model fail");
                            PerfCounter.count("update_model_fail_" + modelPath, 1L, System.currentTimeMillis() - start);
                        }
                        else {
                            modelLastModified = timeStamp;
                            PerfCounter.count("update_model_" + modelPath, 1L, System.currentTimeMillis() - start);
                        }
                    }

                    if (!StringUtil.isEmpty(caliTablePath)) {
                        fileStatus = fs.getFileStatus(new Path(caliTablePath));
                        timeStamp = fileStatus.getModificationTime();
                        if (caliTableLastModified != timeStamp) {
                            long start = System.currentTimeMillis();
                            if (!onCaliTableChanged()) {
                                LOGGER.warn("update calitable fail");
                                PerfCounter.count("update_calitable_fail_" + caliTablePath, 1L, System.currentTimeMillis() - start);
                            }
                            else {
                                caliTableLastModified = timeStamp;
                                PerfCounter.count("update_calitable_" + caliTablePath, 1L, System.currentTimeMillis() - start);
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("update model/calitable fail", e);
                }
            }
        };

        // record fisrt model record
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            FileStatus fileStatus = fs.getFileStatus(new Path(modelPath));
            long timeStamp = fileStatus.getModificationTime();
            modelLastModified = timeStamp;

            if (!StringUtil.isEmpty(caliTablePath)) {
                fileStatus = fs.getFileStatus(new Path(caliTablePath));
                timeStamp = fileStatus.getModificationTime();
                caliTableLastModified = timeStamp;
            }
        } catch (Exception e) {
            LOGGER.error("update model/calitable fail", e);
        }
        Random random = new Random(System.currentTimeMillis());
        scheduler.scheduleAtFixedRate(task, CommonConstant.MODEL_UPDATE_PERIOD + (random.nextInt(CommonConstant.MODEL_UPDATE_PERIOD * 12) % (CommonConstant.MODEL_UPDATE_PERIOD * 12)), CommonConstant.MODEL_UPDATE_PERIOD * 12, TimeUnit.SECONDS);
    }

    /**
     * 子类实现该函数接口用于模型更新
     */
    protected abstract boolean onModelChanged();

    protected abstract TreeMap<Float, Float> readCaliTable(Configuration conf, String caliTablePath);
    /**
     * 子类实现该函数接口用于模型更新
     */
    protected abstract boolean onCaliTableChanged();
    /**
     * @param modelPath 模型文件路径
     * @param period 模型更新检测周期
     *
     * @return 成功加载返回true，否则返回false
     */
    public abstract boolean init(String modelPath, String caliTablePath, Double samplingRate, String modelName);

    /**
     * @param featureValues 特征向量
     *
     * @return 根据模型及特征向量打分后的分值
     */
    public abstract Double score(Set<String> featureValues, String expId, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag);

    public abstract Double score(Map<String,Double> featureMap, String expId, UserData userData, AdData adData, HistoricalDataCorpus historicalDataCorpus, ContextData contextData, String lookBackWindow, Boolean mediaFlag, TypeCollection.Calculation featureType, Boolean optimalFlag);

    public abstract Float getWeight(String key);
}
