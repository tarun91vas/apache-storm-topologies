package bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class YfBolt extends BaseBasicBolt {
    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

        String filename = stormConf.get("fileToWrite").toString();
        try {
            this.writer = new PrintWriter(filename, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Error opening file");
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            Boolean gain = true;

            String company = input.getString(0);
            String timestamp = input.getValue(1).toString();

            Integer price = input.getIntegerByField("price");
            Integer prev_close =  input.getIntegerByField("prev_close");

            if (price < prev_close) {
                gain = false;
            }

            System.out.println("Bolt emitting");
            collector.emit(new Values(company, timestamp, price, gain));
            writer.println(company + ", " + timestamp + ", " + price + ", " +gain);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "timestamp", "price", "gain"));

    }

    @Override
    public void cleanup() {
        super.cleanup();
        writer.close();
    }
}
