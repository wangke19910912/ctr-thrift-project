/**
 * User: wangjinlong
 * Date: 14-11-18
 * Time: 下午11:12
 * DESC: 
 */

package com.codelab.util;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

public class ThriftSerializationHelper {
    private TSerializer serializer;
    private TDeserializer deserializer;

    /**
     * 默认使用compact压缩模式
     */
    public ThriftSerializationHelper() {
        serializer = new TSerializer(new TCompactProtocol.Factory());
        deserializer = new TDeserializer(new TCompactProtocol.Factory());
    }

    /**
     * 兼容其它模式
     *
     * @param protocal TCompactProtocol, TBinaryProtocol, TJSONProtocol
     */
    public ThriftSerializationHelper(TProtocolFactory protocal) {
        serializer = new TSerializer(protocal);
        deserializer = new TDeserializer(protocal);
    }

    /**
     * 序列化
     *
     * @param obj thrift对象
     * @return
     * @throws IOException
     * @throws org.apache.thrift.TException
     */
    public <T extends TBase> byte[] serialize(T obj) throws IOException {
        try {
            return serializer.serialize(obj);
        } catch (TException e) {
            throw new IOException();
        }
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @param target 初始化的空对象
     * @throws org.apache.thrift.TException
     */
    public void deserialize(byte[] bytes, TBase target) throws IOException {
        try {
            deserializer.deserialize(target, bytes);
        } catch (TException e) {
            throw new IOException();
        }
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @param targetClass
     * @return
     * @throws IOException
     */
    public TBase deserialize(byte[] bytes, Class<? extends TBase> targetClass) throws IOException {
        TBase target;
        try {
            target = targetClass.newInstance();
            deserializer.deserialize(target, bytes);
        } catch (InstantiationException e) {
            throw new IOException();
        } catch (IllegalAccessException e) {
            throw new IOException();
        } catch (TException e) {
            throw new IOException();
        }
        return target;
    }
}
