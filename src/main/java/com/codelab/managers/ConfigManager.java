package com.codelab.managers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by wangke on 17/3/22.
 */
public class ConfigManager {
    private static ConfigManager instance = new ConfigManager();
    private Properties properties = new Properties();

    private ConfigManager() {
    }

    public static ConfigManager getInstance() {
        return instance;
    }

    public ConfigManager load(String configFile) {
        InputStream in = ConfigManager.class.getClassLoader().getResourceAsStream(configFile);

        try {
            this.properties.load(in);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            return instance;
        }


    }

    public String getProperty(String propertyName) {
        return this.properties.getProperty(propertyName);
    }

    public String getProperty(String propertyName, String defaultValue) {
        return this.properties.getProperty(propertyName, defaultValue);
    }

    public Properties getProperties() {
        return this.properties;
    }
}

