package com.kafka.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {
        // Create Producer properties
        Properties properties = new Properties();
        // Podemos buscar los nombres de las propiedades en la pagina de Kafka -> Producer Configs
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        // Necesitamos los serializer para convertir las cadenas en bytes de datos
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        // Pasaremos las propiedades que hemos configurado antes
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Vamos a mandar varios mensajes con distintos keys
        for(int i = 0; i < 10; i++) {
            String topic = "primer_topic";
            String value = "Hola mundo " + i;
            String key = "id_" + i;
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            // Podemos incluir una funcion de callback para obtener informacion
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                            "Topic :" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                    }
                }
            });
        }
        // Para asegurar el envio ahora
        producer.flush();
        producer.close();

        // Podemos lanzar un consumer con el siguiente comando
        // kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-app
    }
}
