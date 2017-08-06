package com.james.kafka;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by James on 2017/6/15.
 */
public class Consumers extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    private Queue<String> queue = new ConcurrentLinkedQueue<String>() ;

    public Consumers(String topic) {
        consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.80.32:2181,192.168.80.33:2181,192.168.80.30:2181");
        props.put("group.id", "group1");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");//

        return new ConsumerConfig(props);

    }
    // push消费方式，服务端推送过来。主动方式是pull
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()){
            //逻辑处理
            String msg=new String(it.next().message());
			System.out.println("consumer:"+msg);
            queue.add(msg) ;
        }

    }

    public Queue<String> getQueue()
    {
        return queue ;
    }

    public static void main(String[] args) {
        Consumers consumerThread = new Consumers("track");
        consumerThread.start();
    }
}
