package topologies;

import functions.SplitTextFunc;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;
import spouts.TextSpout;

public class TridentExample {
    public static void  main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();
        topology.newStream("lines", new TextSpout())
                .each(new Fields("line"), new SplitTextFunc(), new Fields("words_split"))
                .each(new Fields("words_split"), new Debug());

        Config config = new Config();
        config.setDebug(true);

        if (args.length != 0 && args[0].equals("remote")) {
            StormSubmitter.submitTopology("Trident-Remote", config, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();

            try {
                cluster.submitTopology("Trident-Local", config, topology.build());
                Thread.sleep(10000);
            } finally {
                cluster.shutdown();
            }

        }

    }
}
