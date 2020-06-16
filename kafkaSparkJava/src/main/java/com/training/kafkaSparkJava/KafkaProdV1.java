package com.training.kafkaSparkJava;

import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProdV1 {

    public static void main(String[] args) {
        System.out.println("yo");

        File file = new File("/home/consultant/Desktop/smallBatch");

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        // these will go in random partition as we increment the key

        String line = " ";

        int key = 0;
        while (true) {
            try {
                if (!((line = br.readLine()) != null)) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            //  System.out.println(line);

            ProducerRecord producerRecord = new ProducerRecord("testJava", Integer.toString(key), line);

            key++;
            System.out.println(key);
            System.out.println(producerRecord);
            kafkaProducer.send(producerRecord);

        }

    }
}
