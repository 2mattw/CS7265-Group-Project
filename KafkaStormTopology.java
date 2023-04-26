import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class KafkaStormTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", createKafkaSpout());
        builder.setBolt("kafkaPrintBolt", new KafkaPrintBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("sumAndCountBolt", new SumAndCountBolt()).shuffleGrouping("kafkaPrintBolt");
        builder.setBolt("averageDelayBolt", new AverageDelayBolt()).shuffleGrouping("sumAndCountBolt");

        Config config = new Config();
        config.put("topology.spout.max.batch.size", 1000);
        config.setDebug(true);
        String topologyName = "kafka-storm-topology";

        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology(topologyName, config, builder.createTopology());
            // Let the topology run for some time (e.g., 1 minute)
            Thread.sleep(6_000_000);
            cluster.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaSpout<String, String> createKafkaSpout() {
        String bootstrapServers = "localhost:9092"; // Replace with your Kafka broker addresses
        String topic = "airline_data"; // Replace with your Kafka topic

        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder(bootstrapServers, topic);
        spoutConfigBuilder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        spoutConfigBuilder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        spoutConfigBuilder.setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-kafka-spout-group");
        spoutConfigBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
//        spoutConfigBuilder.setProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        spoutConfigBuilder.setOffsetCommitPeriodMs(5000);
        spoutConfigBuilder.setRecordTranslator((r) -> new Values(r.key(), r.value()), new Fields("key", "value"));

        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        String fileName = "/Users/matt/Downloads/kafka-storm-output.txt";

        try {
            // Create a FileWriter and a BufferedWriter
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            // Write the content to the file
            bufferedWriter.write(String.valueOf(System.nanoTime()) + System.lineSeparator());

            // Close the BufferedWriter
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }

        return new KafkaSpout<>(spoutConfig);
    }
}

