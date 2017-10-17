package kafka;

import com.google.gson.Gson;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by www-data on 16/8/20.
 */
public class TopicService {

    public int MAX_DETEC_SIZE=10;

    public static final int SOTIMEOUT =100000;
    public static final int BUFFERSIZE=64*1024;
    public static final int MAX_OFFSET_SIZE = 15;
    public static final int MAX_COUNT_SIZE = 200;
    public static final int IN_FACT_OFFSET_SIZE = 10;

    public static AtomicInteger check_size = new AtomicInteger(0);

    public static ConcurrentHashMap<String, TreeMap<Integer, PartitionMetadata>> metaDataContainer = new ConcurrentHashMap<String, TreeMap<Integer, PartitionMetadata>>();


    /**
     * 具体探查数据的方法
     * @param clusterId
     * @param topic
     * @return
     * 2016年3月4日
     */
    public List<Map<String, String>> getDetectiveResultList(String clusterId, String topic) {
        List<Map<String, String>> resultData = new ArrayList<Map<String, String>>();

        String zkAddress = clusterId;//getZkAddressById(clusterId);
        //List<String> seeds = Arrays.asList(brokers.split(","));

        topic ="mobile-pubt-page-event-xg";
        int port =9092;
        List<String> seeds =new ArrayList<String>();
        seeds.add("10.0.42.50");

        OffsetQueryService.PartitionOffset partitionOffset = new OffsetQueryService(topic, port, seeds).getOffsetWithPartitions();

        TreeMap<Integer, PartitionMetadata> metaDatas =findLeader(seeds,port,topic);
        int all_count = 0;
        for (int i=0; i<metaDatas.size(); i++) {
            long readOffset = partitionOffset.getContainer().get(i);
            System.out.println("readOffset = " + readOffset);

            PartitionMetadata metadata = metaDatas.get(i);
            String leadBroker = metadata.leader().host();
            String clientName = "Client_" + topic + "_" + i;

            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);

            FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic,i, readOffset-MAX_OFFSET_SIZE, 1000000000).build();
            FetchResponse fetchResponse = consumer.fetch(req);

            System.out.println("info::::" + fetchResponse);
            System.out.println("metaDatas.size() = " + metaDatas.size());
            System.out.println("fetchResponse.hasError() = " + fetchResponse.hasError());
            if (fetchResponse.hasError()) {

                System.out.println("Error fetching data Offset Data the Broker. Reason: " + fetchResponse.errorCode(topic,i));
                if(i==metaDatas.size()){
                    break;
                }
                continue;
            }

            int count = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic,i)) {
                all_count++;
                if (all_count > MAX_COUNT_SIZE) {
                    break;
                }
                count++;
                //readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String result;
                try {
                    result = new String(bytes, "UTF-8");
                    Map<String, String> parser = new Gson().fromJson(result,HashMap.class);
                    resultData.add(parser);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                if(count == IN_FACT_OFFSET_SIZE){
                    break;
                }
            }

            if (consumer != null){
                consumer.close();
            }
        }
        return resultData;
    }


    public TreeMap<Integer, PartitionMetadata> findLeader(List<String> brokers,int port,String topic) {
        TreeMap<Integer, PartitionMetadata> treeMap =new TreeMap<Integer, PartitionMetadata>();
        for(String broker:brokers){
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(broker, port, SOTIMEOUT, BUFFERSIZE, String.valueOf(broker));
                TopicMetadataRequest tmRequest = new TopicMetadataRequest(Collections.singletonList(topic));
                TopicMetadataResponse tmResponse  =consumer.send(tmRequest);
                List<TopicMetadata> metaData  = tmResponse.topicsMetadata();
                for(TopicMetadata meta:metaData){
                    for(PartitionMetadata part:meta.partitionsMetadata()){
                        treeMap.put(part.partitionId(),part);
                    }
                }
            } catch (Exception e) {
                System.err.println("find leader error....");
            }finally{
                if(consumer!=null){
                    consumer.close();
                }
            }
        }
        return treeMap;
    }



    @Test
    public void  test(){

       List<Map<String,String>> data =getDetectiveResultList("","");
         System.out.println(data);
    }

}
