import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.Collections;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class kafkaProducer {
    private static final Logger logger=LoggerFactory.getLogger(kafkaProducer.class);
    private static final String TOPIC="financial_transactions";
    private static final String BOOTSTRAP_SERVERS="localhost:29092,localhost:39092,localhost:49092";
    private static final int NUM_PARTITIONS=5;
    private static final int NUM_THREADS=3;
    private static final short REPLICATION_FACTOR=3;


    public static void createTopicIfNotExists()
    {
        Properties pros=new Properties();
        pros.put("bootstrap.servers",BOOTSTRAP_SERVERS);
        try(AdminClient adminClient=AdminClient.create(pros))
        {
            boolean topicExists=adminClient.listTopics().names().get().contains(TOPIC);
            if(!topicExists)
            {
                logger.info("Topic "+TOPIC+" does not exist. Creating topic...");
                NewTopic newTopic=new NewTopic(TOPIC,NUM_PARTITIONS,REPLICATION_FACTOR);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("Topic "+TOPIC+" created successfully.");
            }
            else
            {
                logger.info("Topic "+TOPIC+" already exists.");
            }
        }
        catch (Exception e)
        {
            logger.error("Error checking or creating topic ",e);
        }
    }

    public static void main(String[] args) {
        createTopicIfNotExists();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); props.put(ProducerConfig. VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64 KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3); // 5 ms
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        ExecutorService executor = Executors.newFixedThreadPool (NUM_THREADS);
        ObjectMapper objectMapper = new ObjectMapper();

        for(int i=0;i<NUM_THREADS;i++)
        {
            executor.submit(()->{
                long startTime=System.currentTimeMillis();
                long recordSent=0;
                try(KafkaProducer<String,String> producer=new KafkaProducer<>(props)){
                    while(true)
                    {
                        Transaction transaction=Transaction.randomTransaction();
                        String transactionJson=objectMapper.writeValueAsString(transaction);
                        System.out.println(transactionJson);

                        ProducerRecord<String,String> record=new ProducerRecord<>(TOPIC,transaction.getUserId(),transactionJson);
                        producer.send(record,(RecordMetadata metadata,Exception exception)->{
                            if(exception!=null)
                            {
                                logger.error("Failed to send record with key "+record.key()+" due to "+exception.getMessage());
                            }
                            else
                            {
                                logger.info("Record with key "+record.key()+" sent to partition "+ metadata.partition()+" with offset "+metadata.offset());
                            }
                        });
                        recordSent++;

                        // track throughput every second
                        long elapsedTime=System.currentTimeMillis()-startTime;
                        if(elapsedTime>=500)
                        {
                            double throughput=recordSent/(elapsedTime/500.0);
                            logger.info("Throughput "+throughput+" records/sec");
                            recordSent=0;
                            startTime=System.currentTimeMillis();
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.error("Error in producer thread "+e);
                }
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            executor.shutdownNow();
            logger.info("Producer stopped.");
        }));
    }
}
