package com.kafka.tutorial1;

import com.kafka.tutorial1.ConsumerDemoWithThread.ConsumerThread;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="changeme@ext.inditex.com">Jose Gonzalez</a>
 */
public class ConsumerDemoAssignSeek {

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC = "primer_topic";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());



        // Create consumer confir
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // Necesitamos deserializar el mensaje enviado por el producer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0); // Ponemos la particion 0
        long offsetToReadFrom = 15L;
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));

        // Seek
        kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numeroDeMensajes = 5;
        boolean keepOnReading = true;
        int leidos = 0;

        // Obtenemos datos
        while(keepOnReading){
            // Hay que poner un timeout
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                leidos += 1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offser: " + record.offset());
                if(leidos >= numeroDeMensajes){
                    keepOnReading = false;
                    break; // Salimos del for
                }
            }
            logger.info("Salimos del bucle");
        }
    }
}
