package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;

/**
 * Created by www-data on 16/8/16.
 */


public class ConfigWatcher implements Watcher{

    private ActivekeyValueStore store;


    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        store = new ActivekeyValueStore();
        store.connect(hosts);
    }

    public void displayConfig() throws KeeperException, InterruptedException {
        String value = store.read(ConfigUpdater.PATH,this);
        System.out.printf("Read %s as %s \n",ConfigUpdater.PATH,value);
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged){
            try {
                displayConfig();
            } catch (KeeperException e) {
                System.out.printf("keeperException:%s exiting.\n",e);
            } catch (InterruptedException e) {
                System.out.println("Interrupted exiting.");
                Thread.currentThread().interrupt();
            }

        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher configWatcher = new ConfigWatcher("localhost");
        configWatcher.displayConfig();
        Thread.sleep(Long.MAX_VALUE);
    }
}
