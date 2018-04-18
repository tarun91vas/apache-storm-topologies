package functions;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Vector;

public class SplitTextFunc extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for (String word: words) {
            word.trim();
            if (!word.isEmpty()) {
                collector.emit(new Values(word));
            }
        }
    }
}
