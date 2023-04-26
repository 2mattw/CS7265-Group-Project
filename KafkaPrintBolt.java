import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.Map;

public class KafkaPrintBolt extends BaseRichBolt {

    private OutputCollector collector;
    String airport_to_calculate = "SAN";
    List<String> airports_to_calculate = Arrays.asList("SAN", "LAX", "SFO");
    // Use this as the IF statement for multiple airports - airports_to_calculate.contains(columns.get(16))
//    Use this to filter for diverted flights  && (!columns.get(23).equals("1")
    Integer delay;
    String airport;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        String csvRecord = tuple.getStringByField("value");
        if ("__END_OF_FILE__".equals(csvRecord)) {
            System.out.println("End of File");
            collector.emit(new Values("EOF", -1));
            collector.ack(tuple);
        } else {
            List<String> columns = Arrays.asList(csvRecord.split(","));
            if (columns.get(16).equals(airport_to_calculate)) {
                if (!columns.get(15).equals("NA") && !columns.get(23).equals("1")){
                    delay = Integer.valueOf(columns.get(15)); // Replace 0 with the index of the desired column
                    airport = columns.get(16); // Replace 1 with the index of another desired column
                    collector.emit(new Values(airport, delay));
                    collector.ack(tuple);
                }
                collector.ack(tuple);
            }
            collector.ack(tuple);

//            System.out.println("airport=" + airport + " delay=" + delay);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport", "delay"));
    }
}

