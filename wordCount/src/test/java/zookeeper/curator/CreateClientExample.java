package zookeeper.curator;

import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by www-data on 16/8/16.
 */
public class CreateClientExample {

    private static final String PATH = "/kafka_cluster/brokers/topics";
    private static final String hosts = "127.0.0.1:2181";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        try {
            client = createWithOptions(hosts, new ExponentialBackoffRetry(1000, 3), 1000, 1000);
            client.start();
            List<String> children =  client.getChildren().forPath(PATH);
            for (String child:children){
                String data = new String(client.getData().forPath(PATH+"/"+child),CHARSET);
               TopicDataMode dataMode= new Gson().fromJson(data,TopicDataMode.class);
               System.out.printf("child:%s data:%s\n", child, data) ;
                System.out.println(dataMode);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    private static CuratorFramework createClient(String hosts){
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return  CuratorFrameworkFactory.builder().
                connectString(hosts).
                retryPolicy(retryPolicy).
                connectionTimeoutMs(1000).
                sessionTimeoutMs(1000).build();
    }

    public static List<String> conusmerGroupList(String hosts){
        CuratorFramework client = createClient(hosts);
        client.start();
        List<String> consumerGroup = new ArrayList<String>();
        return null;
    }

    public static CuratorFramework createSimple(String connectionString) {
        // these are reasonable arguments for the ExponentialBackoffRetry.
        // The first retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }
    public static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        // using the CuratorFrameworkFactory.builder() gives fine grained control
        // over creation options. See the CuratorFrameworkFactory.Builder javadoc details
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                        // etc. etc.
                .build();
    }

    private  class TopicDataMode{
        private int version;
        private Map<String,List<Integer>> partitions;

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public Map<String, List<Integer>> getPartitions() {
            return partitions;
        }

        public void setPartitions(Map<String, List<Integer>> partitions) {
            this.partitions = partitions;
        }
    }
}
