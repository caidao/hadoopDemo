package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;


import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by www-data on 16/8/16.
 */
public class ConfigUpdater {

    public static final String PATH = "/config";

    private ActivekeyValueStore store;
    private Random random = new Random();

    public ConfigUpdater(String hosts) throws IOException, InterruptedException {
        store = new ActivekeyValueStore();
        store.connect(hosts);
    }

    public void run() throws KeeperException, InterruptedException {
        while (true){
            String value = random.nextInt(100)+"";
            store.write(PATH,value);
            System.out.printf("Set %s to %s\n", PATH, value);
            TimeUnit.SECONDS.sleep(random.nextInt(10));
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigUpdater configUpdater =new ConfigUpdater("localhost");
        configUpdater.run();
    }
}
