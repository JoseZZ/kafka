package com.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="changeme@ext.inditex.com">Jose Gonzalez</a>
 */
public class ConsumerDemoWithThread {

    private static final String GROUP_ID = "group_1";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC = "primer_topic";
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());


    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {

    }

    public void run(){
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creando el hilo del consumer");
        Runnable myConsumer = new ConsumerThread(latch, TOPIC);
        // Arrancamos el hilo
        Thread myHilo = new Thread(myConsumer);
        myHilo.start();

        // Creamos un hook para apagar
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Shutdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            try{
                latch.await();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("La aplicacion ha terminado");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("La aplicacion se se ha interrumplico", e);
        } finally {
            logger.info("La aplicacion se esta cerrando");
        }
    }

    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> kafkaConsumer;
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(CountDownLatch latch, String topic){
            this.latch = latch;

            // Create consumer confir
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
            // Necesitamos deserializar el mensaje enviado por el producer
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);

            // Subscribe consumer to our topic(s)
            // Por ahora solo nos vamos a subscribir a un topic, por eso una lista de un elemento
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        }

        @Override
        public void run() {
            // Obtenemos datos
            try {
                while (true) {
                    // Hay que poner un timeout
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offser: " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Recibida señal de apagado");
            } finally {
                kafkaConsumer.close();
                // Le decimos al main que lo hemos hecho con el consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // Con este metodo interrumpimos consumer.poll()
            // Lanza la exception WakeUpException
            kafkaConsumer.wakeup();
        }
    }
}
