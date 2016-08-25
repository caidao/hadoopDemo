package kafka;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by www-data on 16/8/18.
 */
public class MetaDataDump {
    public static void main(String[] args) {
        kafka.javaapi.consumer.SimpleConsumer consumer  = new SimpleConsumer("localhost",
                9094,
                100000,
                64 * 1024, "test2");
        List<String> topics2 = new ArrayList<String>();
        TopicMetadataRequest req = new TopicMetadataRequest(topics2);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        List<kafka.javaapi.TopicMetadata> data3 =  resp.topicsMetadata();
        for (kafka.javaapi.TopicMetadata item : data3) {
            System.out.println("Topic: " + item.topic());
            for (PartitionMetadata part: item.partitionsMetadata() ) {
                String replicas = "";
                String isr = "";
                for (BrokerEndPoint replica: part.replicas() ) {
                    replicas += " " + replica.host();
                }
                for (BrokerEndPoint replica: part.isr() ) {
                    isr += " " + replica.host();
                }
                System.out.println( "    Partition: " +   part.partitionId()  + ": Leader: " + part.leader().host() + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
            }
        }
    }
}
