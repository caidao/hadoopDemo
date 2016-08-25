package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by www-data on 16/8/15.
 */
public class JoinGroup extends CreateGroup{


    public void join(String groupName,String memberName) throws KeeperException, InterruptedException {
        String path ="/"+groupName+"/"+memberName;
        String createdPath = zk.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("create : "+createdPath);
    }

    @Test
    public void test() throws IOException, InterruptedException, KeeperException {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect("localhost");
        joinGroup.join("zoo", "test2");
        Thread.sleep(Long.MAX_VALUE);
    }


}
