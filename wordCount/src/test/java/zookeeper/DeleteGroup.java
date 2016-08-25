package zookeeper;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by www-data on 16/8/15.
 */
public class DeleteGroup extends CreateGroup{

    public void delete(String groupName){
        String path="/"+groupName;
        try{
            List<String> children = zk.getChildren(path,false);
            for (String child:children){
                zk.delete(path+"/"+child,-1);
            }
            zk.delete(path,-1);
        }catch (Exception ex){

        }
    }

    @Test
    public void test() throws IOException, InterruptedException {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect("localhost");
        deleteGroup.delete("zoo");
        deleteGroup.close();
    }
}
