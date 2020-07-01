package com.oujian;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class KafkaStreaming {
    public static void main(String[] args) {
        String brokers="hadoop100:9092";
        String zookeeper="hadoop100:2181";

        String form="log";
        String to ="recommender";

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);
        StreamsConfig streamsConfig = new StreamsConfig(properties);
        //拓扑构建器
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("SOURCE",form)
                .addProcessor("PROCESS",()->new LogProcess(),"SOURCE")
                .addSink("SINK",to,"PROCESS");
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, streamsConfig);


    }

}
