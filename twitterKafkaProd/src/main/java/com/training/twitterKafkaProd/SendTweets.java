package com.training.twitterKafkaProd;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;

public class SendTweets {

    public SendTweets(String message){

        // this is constructor class for Kafka Producer
        // not sure this format makes sense :)

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // need to send message as producer record object

        ProducerRecord producerRecord = new ProducerRecord("test", message);

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        kafkaProducer.send(producerRecord);
        kafkaProducer.close();


    }

    public static void main(String[] args) {

        System.out.println("this class is going to send tweets to the Kafka Producer");

        // streaming tweets
        FilterQuery query;

        // here I'm creating the twitterStream
        TwitterStream twitterStream = new TwitterStreamFactory()
                .getInstance()
                .addListener(new StatusListener() {

                    // define counter variable count
                    Integer count = 1;

                    public void onStatus(Status status) {

                        // print status json and increment counter
                        System.out.println(status);
                        System.out.println("");
                        System.out.println(count);
                        count += 1;
                        System.out.println("");

                        // for each status want to send as kafka message
                        // here I'm instantiating SendTweets and sending the message
                        String stringStatus = status.toString();
                        SendTweets sendTweets = new SendTweets(stringStatus);

                    }

                    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

                    }

                    public void onTrackLimitationNotice(int i) {

                    }

                    public void onScrubGeo(long l, long l1) {

                    }

                    public void onStallWarning(StallWarning stallWarning) {

                    }

                    public void onException(Exception e) {

                    }
                }).filter(new FilterQuery("covid"));

    }

}
