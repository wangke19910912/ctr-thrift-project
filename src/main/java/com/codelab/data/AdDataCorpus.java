package com.codelab.data;

import com.codelab.entity.AdData;
import com.codelab.util.CommonConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class AdDataCorpus {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdDataCorpus.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private long lastModified;
    private String templatePath;  // 模型文件路径
    private Map<Long, AdData> materialMap = new ConcurrentHashMap<Long, AdData>();
    private Map<Long, AdData> adMap = new ConcurrentHashMap<Long, AdData>();
    private static AdDataCorpus adTemplate = null;

    public AdDataCorpus() {
    }

    public static synchronized AdDataCorpus getInstance() {
        if (adTemplate == null)
            adTemplate = new AdDataCorpus();
        return adTemplate;
    }

    public boolean init(String templatePath) {
        try {
            this.templatePath = templatePath;
            LOGGER.info("ad data path: {}", templatePath);
            boolean ret = readAdData(new Configuration(), templatePath);
            if (!ret) {
                LOGGER.error("ad data init error, return false ");
                return false;
            }
            start();
        } catch (Exception e) {
            LOGGER.error("AdDataCorpus init failed : {}", e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 定期检测模型文件是否发生变化，若发生变化则进行模型更新
     */
    private void start() {
        final Runnable task = new Runnable() {
            public void run() {
                try {
                    Configuration conf = new Configuration();
                    FileSystem fs = FileSystem.get(conf);
                    FileStatus fileStatus = fs.getFileStatus(new Path(templatePath));
                    long timeStamp = fileStatus.getModificationTime();
                    if (lastModified != timeStamp) {
                        lastModified = timeStamp;
                        readAdData(conf, templatePath);
                    }
                } catch (Exception e) {
                    LOGGER.error("update adInfo fail", e);
                }
            }
        };
        scheduler.scheduleAtFixedRate(task, CommonConstant.AD_CORPUS_UPDATE_PERIOD, CommonConstant.AD_CORPUS_UPDATE_PERIOD, TimeUnit.SECONDS); //
    }

    protected static AdData create() {
        AdData adData = new AdData();
        return adData;
    }

    private boolean readAdData(Configuration conf, String templatePath) {
        Map<Long, AdData> tempMaterialMap = new ConcurrentHashMap<Long, AdData>();
        Map<Long, AdData> tempAdLevelMap = new ConcurrentHashMap<Long, AdData>();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (fs == null) {
            LOGGER.error("file system init failed");
            return false;
        }
        Path inFile = new Path(templatePath);

        if (adMap.size() <= tempAdLevelMap.size()) {
            adMap = tempAdLevelMap;
        } else {
            LOGGER.error("ad level size normal: {}, {}", adMap.size(), tempAdLevelMap.size());
            return false;
        }
        if (materialMap.size() <= tempMaterialMap.size()) {
            materialMap = tempMaterialMap;
        } else {
            LOGGER.error("material level not normal: {}, {}", materialMap.size(), tempMaterialMap.size());
            return false;
        }
        return true;
    }


}
