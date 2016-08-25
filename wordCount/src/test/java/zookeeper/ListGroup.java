package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by www-data on 16/8/15.
 */
public class ListGroup extends CreateGroup{

    public void list(String groupName){
        String path ="/"+groupName;
        try{
            List<String> children =zk.getChildren(path,false);
            if (children==null||children.isEmpty()){
                System.out.println("no member in group!");
                return;
            }

            for (String chid:children){
                System.out.println(chid);
            }

        }catch (Exception ex){

        }
    }

    @Test
    public  void test() throws KeeperException, InterruptedException, IOException {
        ListGroup listGroup = new ListGroup();
        listGroup.connect("127.0.0.1:2181");
        listGroup.list("kafka_cluster");
        listGroup.close();
    }
}
