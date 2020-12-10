import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Application {
    private static final List <String> TOPICS = Arrays.asList("valid-transactions", "suspicious-transactions");//may change this
    //private static final String TOPIC1 = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        //*****************
        // YOUR CODE HERE
        //*****************
        Transaction transaction = new Transaction();
        Application kafkaConsumerApp = new Application();

        String consumerGroup = "reporting-service";
        if (args.length == 1) {//if user passes in cmd line parameter use this otherwise use default above
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        kafkaConsumerApp.consumeMessages(TOPICS, kafkaConsumer);
        //recordTransactionForReporting(String , transaction);//need to pass in correct argument not entirely happy with list above
        System.out.println("Placeholder for method, when I work out correct list items");
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        //*****************
        // YOUR CODE HERE
        //*****************
        //kafkaConsumer.subscribe(Collections.singletonList(topics));
        kafkaConsumer.subscribe(Collections.synchronizedList(topics));//not sure about synchronized list

        while(true){
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if(consumerRecords.isEmpty()){

            }
            for(ConsumerRecord<String, Transaction> record: consumerRecords){
                System.out.println(String.format("Received record (key: %s, value: %s, partition: %d, offset: %d",
                        record.key(),record.value(), record.partition(), record.offset()));
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        //*****************
        // YOUR CODE HERE
        //*****************
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);//how the consumer can access the cluster
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//turning auto commit off.

      return new KafkaConsumer<>(properties);
   }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        //*****************
        // YOUR CODE HERE
        //*****************
        System.out.println("All records go here");
        if(TOPICS.contains("valid-transactions")){
            System.out.println("valid transaction for user %s"+ transaction.getUser());
        }
        else{
            System.out.println("invalid transaction for user %s" + transaction.getUser());
        }
    }

}
