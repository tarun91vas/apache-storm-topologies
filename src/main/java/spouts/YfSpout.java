package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Random;

public class YfSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Random rand = new Random();

            Integer price = rand.nextInt(100);
            Integer prev_close = rand.nextInt(100);

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            System.out.println("Spout emitting");
            collector.emit(new Values("MSFT", timestamp, price, prev_close));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "timestamp", "price", "prev_close"));
    }
}
