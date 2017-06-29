package com.codelab.util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaMsgSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMsgSender.class);
    private static Producer<String, byte[]> byteProducer = null;
    private static Producer<String, String> stringProducer = null;
    private static ConfigManager configManager = ConfigManager.getInstance();
    private static KafkaMsgSender kafkaMsgSender = null;

    private KafkaMsgSender() {
    }

    public static synchronized KafkaMsgSender getByteSender() {

        if (kafkaMsgSender == null) {
            kafkaMsgSender = new KafkaMsgSender();
            if (!kafkaMsgSender.isByteProducerInit())
                kafkaMsgSender.initByteProducer();
        }
        return kafkaMsgSender;
    }

    public static synchronized KafkaMsgSender getStringSender() {
        if (kafkaMsgSender == null) {
            kafkaMsgSender = new KafkaMsgSender();
            if (!kafkaMsgSender.isStringProducerInit())
                kafkaMsgSender.initStringProducer();
        }
        return kafkaMsgSender;
    }

    public synchronized boolean isByteProducerInit() {
        if (byteProducer == null)
            return false;
        else
            return true;
    }

    public synchronized boolean isStringProducerInit() {

        if (stringProducer == null)
            return false;
        else
            return true;
    }

    public void initByteProducer() {
        if (null == byteProducer) {
            synchronized (this) {
                if (null == byteProducer) {
                    LOGGER.debug("Begin to init kafka producer:");

                    Properties properties = new Properties();
                    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
                    String kafkaBrokerListVal = configManager.load("kafka.properties").getProperty("kafka_broker_list");
                    LOGGER.info("Produce messages to kafka cluster: " + kafkaBrokerListVal);

                    LOGGER.info("kafkaBrokerListVal: {}", kafkaBrokerListVal);
                    properties.put("metadata.broker.list", kafkaBrokerListVal);
//                    设置为异步模式
                    properties.put("request.required.acks", "0");
                    properties.put("producer.type", "async");
//                    设置消息的重试次数为1，防止多次上传日志
                    properties.put("message.send.max.retries", "1");
//                    设置请求过期时间为10ms
                    properties.put("request.timeout.ms", "10");
//                    批处理的数量和间隔时间，按每秒x条日志来算
                    properties.put("batch.num.messages", "10");
                    properties.put("queue.buffering.max.ms", "5000");


                    LOGGER.debug("Current properties:{}", properties);

                    ProducerConfig producerConfig = new ProducerConfig(properties);

                    byteProducer = new Producer<String, byte[]>(producerConfig);
                    LOGGER.debug("Finish to init kafka producer:");
                }
            }
        }
    }

    public void initStringProducer() {
        if (null == stringProducer) {
            synchronized (this) {
                if (null == stringProducer) {
                    LOGGER.debug("Begin to init kafka producer:");

                    Properties properties = new Properties();
                    properties.put("serializer.class", "kafka.serializer.StringEncoder");
                    String kafkaBrokerListVal = configManager.load("kafka.properties").getProperty("kafka_broker_list");
                    LOGGER.info("Produce messages to kafka cluster: " + kafkaBrokerListVal);

                    LOGGER.info("kafkaBrokerListVal: {}", kafkaBrokerListVal);
                    properties.put("metadata.broker.list", kafkaBrokerListVal);
                    //设置为异步模式
                    properties.put("request.required.acks", "0");
                    properties.put("producer.type", "async");
                    // 设置消息的重试次数为1，防止多次上传日志
                    properties.put("message.send.max.retries", "1");
                    //设置请求过期时间为10ms
                    properties.put("request.timeout.ms", "10");
                    //批处理的数量和间隔时间，按每秒x条日志来算
                    properties.put("batch.num.messages", "10");
                    properties.put("queue.buffering.max.ms", "5000");

                    LOGGER.debug("Current properties:{}", properties);

                    ProducerConfig producerConfig = new ProducerConfig(properties);

                    stringProducer = new Producer<String, String>(producerConfig);
                    LOGGER.debug("Finish to init kafka producer:");
                }
            }
        }
    }

    public void sendMsg(KafkaTopics kafkaTopic, String key, String content) {
        if (null == stringProducer) {
            LOGGER.error("Msg receiver is null! {}", content);
            return;
        }

        try {
            stringProducer.send(new KeyedMessage<String, String>(kafkaTopic.name(), key, content));
        } catch (Exception e) {
            LOGGER.error("Failed to send msg:{}. {}", content, e);
        }
        LOGGER.debug("kafkaContent: {}, key: {}", content, key);
    }

    public void sendMsg(KafkaTopics kafkaTopic, String content) {
        if (null == stringProducer) {
            LOGGER.error("Msg receiver is null! {}", content);
            return;
        }

        try {
            String name = kafkaTopic.name();
            stringProducer.send(new KeyedMessage<String, String>(kafkaTopic.name(), content));
        } catch (Exception e) {
            LOGGER.error("Failed to send msg:{}. {}", content, e);
        }
        LOGGER.debug("kafkaContent: {}", content);
    }

    public void sendMsg(KafkaTopics kafkaTopic, String key, byte[] content) {

        if (null == byteProducer) {
            LOGGER.error("Msg receiver is null! {}", content);
            return;
        }

        try {
            byteProducer.send(new KeyedMessage<String, byte[]>(kafkaTopic.name(), key, content));
        } catch (Exception e) {
            LOGGER.error("Failed to send msg:{}. {}", content, e);
        }
        LOGGER.debug("kafkaContent: {}, key: {}", content, key);
    }

    public void sendMsg(KafkaTopics kafkaTopic, byte[] content) {

        if (null == byteProducer) {
            LOGGER.error("Msg receiver is null! {}", content);
            return;
        }

        try {
            byteProducer.send(new KeyedMessage<String, byte[]>(kafkaTopic.name(), content));
        } catch (Exception e) {
            LOGGER.error("Failed to send msg:{}. {}", content, e);
        }
        LOGGER.debug("kafkaContent: {}, isEnableExpose: {}", content);
    }

    public void close() {
        if (byteProducer != null)
            byteProducer.close();
        if (stringProducer != null)
            stringProducer.close();
    }
}
