package com.james.kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;

import java.util.Properties;
import java.util.Random;
/**
 * Created by James on 2017/6/15.
 */
public class Producers extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public Producers(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");// 字符串消息
        props.put("metadata.broker.list","192.168.80.32:9092");
        // Use random partitioner. Don't need the key type. Just set it to
        // Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        // order_id,order_amt,create_time,area_id
        Random random = new Random();
        String[] order_amt = { "10.10", "20.10", "50.2","60.0", "80.1","92.3" };
        String[] area_id = { "1","2","3","4","5","6","7","8","9","10" };

        int i =0 ;
        while(true) {
            i ++ ;
            String messageStr = i+"\t"+order_amt[random.nextInt(6)]+"\t"+DateFmt.getCountDate(null, DateFmt.date_long)+"\t"+area_id[random.nextInt(10)] ;
            System.out.println("product:"+messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));

			Utils.sleep(1000) ;
            if (i==10) {
                break;
            }
        }

    }

    public static void main(String[] args) {
        Producers producersThread = new Producers("track");
        producersThread.start();
    }
}
