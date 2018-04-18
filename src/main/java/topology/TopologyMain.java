package topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import bolts.YfBolt;
import spouts.YfSpout;

public class TopologyMain {
    public static void main(String[] args) throws Exception{
        //Build topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("YF-Spout", new YfSpout(), 3);
        builder.setBolt("YF-Bolt", new YfBolt(), 3).shuffleGrouping("YF-Spout");

        StormTopology topology = builder.createTopology();

        //Configure
        Config conf = new Config();
        //conf.setDebug(true);
        conf.put("fileToWrite", "/Users/z002n11/projects/stock-price-tracker/output.txt");

        //Submit topology
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology("STOCK-PRICE-TRACKER", conf, topology);
            Thread.sleep(10000);
        } finally {
            cluster.shutdown();
        }
    }

}
