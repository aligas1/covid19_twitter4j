package com.training.kafkaSparkJava;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProd {

    public SimpleProd(){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // need to send message as producer record object

        ProducerRecord producerRecord = new ProducerRecord("test", "this is a message");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }

    public static void main(String[] args) {

//        System.out.println("Hi");

        SimpleProd simpleProd = new SimpleProd();

    }

}
