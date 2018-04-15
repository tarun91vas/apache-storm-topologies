import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

public class YfSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            StockQuote quote = YahooFinance.get("MSFT").getQuote();

            BigDecimal price = quote.getPrice();
            BigDecimal prev_close = quote.getPreviousClose();

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            collector.emit(new Values("MSFT", timestamp, price.doubleValue(), prev_close.doubleValue()));

        } catch (Exception e) {

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "timestamp", "price", "prev_close"));
    }
}
