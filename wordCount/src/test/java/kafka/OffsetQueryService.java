package kafka;

import com.google.gson.Gson;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.io.Serializable;
import java.util.*;

/**
 * Created by www-data on 16/8/20.
 */
public class OffsetQueryService implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4286046944947892454L;
    public static final int SOTIMEOUT =100000;
    public static final int BUFFERSIZE=64*1024;
    public static final int MAXNUMOFFSETS =1;

    /**
     * topic
     */
    private String topic;

    /**
     * kafka node port,for example:9092
     */
    private int port;

    /**
     * kafka broker list
     */
    private List<String> seeds;

    public OffsetQueryService(String topic,int port,List<String> seeds){
        this.topic = topic;
        this.port = port;
        this.seeds = seeds;
    }



    /**
     * get the offset information of all partition
     * @return
     */
    public PartitionOffset getOffsetWithPartitions(){
        PartitionOffset partitionOffset = new PartitionOffset();
        TreeMap<Integer, PartitionMetadata> metaDatas = findLeader(this.seeds, this.port, this.topic);
        if(metaDatas==null||metaDatas.size()==0){
            partitionOffset.setStatus(false);
            return partitionOffset;
        }
        Map<Integer, Long> containers = new HashMap<Integer, Long>();
        for(Map.Entry<Integer, PartitionMetadata> entry:metaDatas.entrySet()){
            int partition = entry.getKey();
            String leaderBroker = entry.getValue().leader().host();
            String clientName = topic+"_"+partition;
            SimpleConsumer consumer = new SimpleConsumer(leaderBroker, port, SOTIMEOUT, BUFFERSIZE, clientName);
            long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
            if(consumer!=null){
                consumer.close();
            }
            containers.put(partition, readOffset);
        }
        partitionOffset.setContainer(containers);
        return partitionOffset;
    }

    public PartitionOffset getOffsetWithPartitions(int partitionId){
        PartitionOffset partitionOffset = new PartitionOffset();
        TreeMap<Integer, PartitionMetadata> metaDatas = findLeader(this.seeds, this.port, this.topic);
        if(metaDatas==null||metaDatas.size()==0){
            partitionOffset.setStatus(false);
            return partitionOffset;
        }
        Map<Integer, Long> containers = new HashMap<Integer, Long>();
        for(Map.Entry<Integer, PartitionMetadata> entry:metaDatas.entrySet()){
            int partition = entry.getKey();
            long readOffset=-1;
            if (partitionId==partition) {
                String leaderBroker = entry.getValue().leader().host();
                String clientName = topic + "_" + partition;
                SimpleConsumer consumer = new SimpleConsumer(leaderBroker, port, SOTIMEOUT, BUFFERSIZE, clientName);
                 readOffset= getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                if (consumer != null) {
                    consumer.close();
                }
            }
            containers.put(partition, readOffset);
        }
        partitionOffset.setContainer(containers);
        return partitionOffset;
    }




    /**
     * <p>
     *  If you want to use low level API: SimpleConsumer,you must find the leader broker
     * </p>
     * @param brokers
     * @param port
     * @param topic
     * @return
     */
    private TreeMap<Integer, PartitionMetadata> findLeader(List<String> brokers,int port,String topic){
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

    /**
     *
     * @param consumer
     * @param topic
     * @param partition
     * @param time
     * @param clientName
     * @return
     */
    private long getLastOffset(SimpleConsumer consumer,String topic,int partition,long time,String clientName){
        return getOffsetWithTime(consumer, topic, partition, time, clientName);
    }

    /**
     * get the offset with defined long time
     * @return
     */
    public long getOffsetWithTime(SimpleConsumer consumer,String topic,int partition,long time,String clientName){
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time,MAXNUMOFFSETS));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()){
            System.err.println("fetching offset from the broker error..."+response.toString());
            return -2;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public static void main(String[] args){
        String topic ="paner_new1";
        int port =9092;
        List<String> seeds = new ArrayList<String>();
        seeds.add("127.0.0.1");
        OffsetQueryService ss = new OffsetQueryService(topic, port, seeds);
        PartitionOffset partitionOffset=ss.getOffsetWithPartitions(1);
        System.out.println(partitionOffset);
    }

    public class PartitionOffset implements Serializable{

        /**
         *
         */
        private static final long serialVersionUID = -6219450168795132984L;


        private boolean status;

        private Map<Integer, Long> container;

        public PartitionOffset(){
            this(true);
        }

        public PartitionOffset(boolean status){
            this.status = status;
        }

        public PartitionOffset(boolean status,Map<Integer, Long> container){
            this(status);
            this.container = container;
        }

        public boolean isStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public Map<Integer, Long> getContainer() {
            return container;
        }

        public void setContainer(Map<Integer, Long> container) {
            this.container = container;
        }

        @Override
        public String toString() {
            return new Gson().toJson(this);
        }

    }
}