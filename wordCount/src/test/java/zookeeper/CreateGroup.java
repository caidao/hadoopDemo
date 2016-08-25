package zookeeper;

import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by www-data on 16/8/15.
 */
public class CreateGroup implements Watcher {

    private static final int SEESION_TIMEOUT = 5000;

    protected ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts,SEESION_TIMEOUT,this);
        connectedSignal.await();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }

    public  void create(String groupName) throws KeeperException, InterruptedException {
        String path = "/"+groupName;
        String createdPath = zk.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("created : "+createdPath);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    @Test
    public  void test1() throws IOException, InterruptedException, KeeperException {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect("localhost");
        createGroup.create("zoo");
        createGroup.close();

    }
}
