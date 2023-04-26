import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AverageDelayBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, List<Integer>> airportDataMap;

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.airportDataMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String airport = tuple.getStringByField("airport");
        int sum = tuple.getIntegerByField("sum");
        int count = tuple.getIntegerByField("count");

        if ("EOF".equals(airport)) {

            for (Map.Entry<String, List<Integer>> entry : airportDataMap.entrySet()) {
                String temp_airport = entry.getKey();
                List<Integer> data = entry.getValue();
                int temp_sum = data.get(0);
                int temp_count = data.get(1);
                double average = (double) temp_sum / temp_count;

                String fileName = "/Users/matt/Downloads/kafka-storm-output.txt";

                try {
                    // Create a FileWriter and a BufferedWriter
                    FileWriter fileWriter = null;
                    try {
                        fileWriter = new FileWriter(fileName, true);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

                    // Write the content to the file
                    bufferedWriter.write("Airport:" + temp_airport + " Avg Dep. Delay: " + average + " count:" + temp_count + " sum:" + temp_sum  + System.lineSeparator());
                    bufferedWriter.write(String.valueOf(System.nanoTime()));

                    // Close the BufferedWriter
                    bufferedWriter.close();
                } catch (IOException e) {
                    System.err.println("Error writing to file: " + e.getMessage());
                }
            }

        } else {
            if (airportDataMap.containsKey(airport)) {
                // Key exists, update the sum and count
                List<Integer> data = airportDataMap.get(airport);
                data.set(0, sum);   // Update the sum (index 0)
                data.set(1, count); // Update the count (index 1)
            } else {
                // Key does not exist, add a new entry
                List<Integer> data = new ArrayList<>();
                data.add(sum);   // Add the sum (index 0)
                data.add(count); // Add the count (index 1)

                airportDataMap.put(airport, data);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No need to declare output fields since we're not emitting any tuple
    }
}

