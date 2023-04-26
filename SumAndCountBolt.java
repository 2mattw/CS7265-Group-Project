import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class SumAndCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> airportDelaySum;
    private Map<String, Integer> airportDelayCount;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.airportDelaySum = new HashMap<>();
        this.airportDelayCount = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String airport = tuple.getStringByField("airport");
        int delay = tuple.getIntegerByField("delay");

        if ("EOF".equals(airport)) {
            // Pass the "end of file" tuple to the next bolt
            collector.emit(new Values("EOF", -1, -1));
        } else {
            airportDelaySum.put(airport, airportDelaySum.getOrDefault(airport, 0) + delay);
            airportDelayCount.put(airport, airportDelayCount.getOrDefault(airport, 0) + 1);

            collector.emit(new Values(airport, airportDelaySum.get(airport), airportDelayCount.get(airport)));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport", "sum", "count"));
    }
}

