package com.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.util.resources.cldr.kam.CalendarData_kam_KE;

/**
 * @author <a href="changeme@ext.inditex.com">Jose Gonzalez</a>
 */
public class ConsumerDemo {

    private static final String GROUP_ID = "my-java-app";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC = "primer_topic";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());



        // Create consumer confir
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // Necesitamos deserializar el mensaje enviado por el producer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // Subscribe consumer to our topic(s)
        // Por ahora solo nos vamos a subscribir a un topic, por eso una lista de un elemento
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        // Obtenemos datos
        while(true){
            // Hay que poner un timeout
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offser: " + record.offset());
            }
        }
    }
}
