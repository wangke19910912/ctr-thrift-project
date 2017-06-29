package com.codelab.util;

import java.util.Map;

/**
 * Created by Hongang on 2015/10/10.
 */
public interface ICommon {
    public boolean init(Object obj);
    public Object getKey(Object key);
    public Map<String, String> getAll();
}
