package kafka;

import joptsimple.OptionSpecBuilder;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand$;
import kafka.utils.ZkUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;
import scala.runtime.Nothing$;

import java.util.Calendar;

/**
 * Created by www-data on 16/8/18.
 */
public class TopicDemo {

    @Test
    public void describe(){
        int time = Calendar.getInstance().get(Calendar.SECOND);
        String[] options = new String[]{
                "--describe",
                "--zookeeper",
                "localhost:2181",
                "--topic",
                "paner123"
        };

    //   oper(options);
       ZkUtils zkUtils= ZkUtils.apply("localhost:2181", 3000, 3000, true);
        TopicCommand.describeTopic(zkUtils, new TopicCommand.TopicCommandOptions(options));
        System.out.println("-----");


    }


    public static void oper(String args[]){
        try {
            TopicCommand$ topicCommand$ = TopicCommand$.MODULE$;
            final TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(args);
            if(args.length == 0) {
                throw kafka.utils.CommandLineUtils$.MODULE$.printUsageAndDie(opts.parser(), "Create, delete, describe, or change a topic.");
            } else {
                int actions =0;
                OptionSpecBuilder[] optionSpecBuilders = {opts.createOpt(), opts.listOpt(), opts.alterOpt(), opts.describeOpt(), opts.deleteOpt()};
                for (OptionSpecBuilder temp:optionSpecBuilders){
                    if (opts.options().has(temp)) {
                        actions++;
                    }
                }
                if(actions != 1) {
                    throw kafka.utils.CommandLineUtils$.MODULE$.printUsageAndDie(opts.parser(), "Command must include exactly one action: --list, --describe, --create, --alter or --delete");
                } else {
                    opts.checkArgs();
                    ZkUtils zkUtils = kafka.utils.ZkUtils$.MODULE$.apply((String)opts.options().valueOf(opts.zkConnectOpt()), 30000, 30000, JaasUtils.isZkSecurityEnabled());
                    byte exitCode = 0;
                    try {
                        try {
                            if(opts.options().has(opts.createOpt())) {
                                topicCommand$.createTopic(zkUtils, opts);
                            } else if(opts.options().has(opts.alterOpt())) {
                                topicCommand$.alterTopic(zkUtils, opts);
                            } else if(opts.options().has(opts.listOpt())) {
                                topicCommand$.listTopics(zkUtils, opts);
                            } else if(opts.options().has(opts.describeOpt())) {
                                topicCommand$.describeTopic(zkUtils, opts);
                            } else if(opts.options().has(opts.deleteOpt())) {
                                topicCommand$.deleteTopic(zkUtils, opts);
                            }
                        } catch (final Throwable var12) {
                            scala.Predef$.MODULE$.println((new StringBuilder()).append("Error while executing topic command : ").append(var12.getMessage()).toString());
                            System.out.println(var12);
                            exitCode = 1;
                            return;
                        }
                    } finally {
                        zkUtils.close();
//                    System.exit(exitCode);
                    }
                }
            }
        } catch (Nothing$ nothing$) {
            nothing$.printStackTrace();
        }
    }
}
