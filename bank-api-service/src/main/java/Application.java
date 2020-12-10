import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {

    private static final String TOPIC = "valid-transactions";//
    private static final String TOPIC1 = "suspicious-transactions";//
    private static final String TOPIC2 = "high-value";//
    //only need 9092 but adding the other local hosts in case it goes down
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final double HIGH_VALUE_AMT = 1000.00;


    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();
        //Transaction transaction = new Transaction();
        Application kafkaApplication = new Application();
        Producer<String, Transaction> kafkaProducer = kafkaApplication.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(incomingTransactionsReader, customerAddressDatabase,kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally{
            kafkaProducer.flush();//clears the producer
            kafkaProducer.close();//close any network connections formed
        }

    }
    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {
        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction
        // location match or not.
        // Print record metadata information

        int i = 0;
        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();

            String key = transaction.getUser();
            String userResidence = customerAddressDatabase.getUserResidence(transaction.getUser());
            if(transaction.getTransactionLocation().equalsIgnoreCase(userResidence))
            {
              ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC,key, transaction);//not sure this is correct
              RecordMetadata recordMetadata = kafkaProducer.send(record).get();//futures async call
               System.out.println(String.format("Record with (keys: %s, value: %s) was sent to (partition: %d, offset: %d, topic: %s",
                      record.key(),
                      record.value(), recordMetadata.partition(), recordMetadata.offset(),record.topic()));
            }
            else {
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC1,key, transaction);
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                //key is user
                System.out.println(String.format("Record with (keys: %s, value: %s) was sent to (partition: %d, offset: %d, topic: %s", record.key(),
                        record.value(), recordMetadata.partition(), recordMetadata.offset(), record.topic()));
            }
            //decided to have a separate if statement for the higher value topic as Acc mgr needs to be notified for all valid amts
            //considered a nested if, but a separate if seemed the most straight forward.
            if(transaction.getTransactionLocation().equalsIgnoreCase(userResidence) && transaction.getAmount() > HIGH_VALUE_AMT){
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC2,key, transaction);
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                //key is user
                System.out.println(String.format("Record with (keys: %s, value: %s) was sent to (partition: %d, offset: %d, topic: %s", record.key(),
                        record.value(), recordMetadata.partition(), recordMetadata.offset(), record.topic()));
            }

        }
    }

    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
    //*****************
    // YOUR CODE HERE
    //*****************
       Properties properties= new Properties();
       properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);//key = BOOTSTRAP_SERVERS, value = bootstrapServers
       //Give producer a name
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transactions-producer");//uniquely id each individual producer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//any kind of object can be used as an key or value, need to tell Kafka how to serialise those objects into byte streams
       properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

       return new KafkaProducer<String, Transaction>(properties);




  }

}
