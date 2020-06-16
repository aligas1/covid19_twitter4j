//package com.training.kafkaSparkJava;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
//
//import twitter4j.Twitter;
//import twitter4j.TwitterFactory;
//import twitter4j.conf.ConfigurationBuilder;
//import twitter4j.Lis
//
//
//import java.util.Properties;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//
//public class TwitterProd {
//
//    public static void PushTwitterMessage() {
//
//        String consumerKey="x";
//        String consumerSecret="x";
//        String token="x";
//        String secret="x";
//        KeyedMessage<String, String> message = null;
//
//        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
//        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
//        // filter list
//        endpoint.trackTerms(Lists.newArrayList("covid-19"));
//
//        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
//
//        // create a new BasicClient
//        Client client = new ClientBuilder()
//                .hosts(Constants.STREAM_HOST)
//                .endpoint(endpoint)
//                .authentication(auth)
//                .processor(new StringDelimitedProcessor(queue))
//                .build();
//
//        // Establish a connection
//        client.connect();
//
//        // do whatever needs to be done with messages
//        for (int msgRead = 0; msgRead < 1000; msgRead++) {
//
//            try{
//                String msg = queue.take();
//                System.out.println(msg);
//                message = new KeyedMessage<String, String>(topic, queue.take());
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            producer.send(message);
//        }
//        producer.close();
//        client.stop();
//    }
//
//    public TwitterProd(){
//        ConfigurationBuilder cb = new ConfigurationBuilder();
//        cb.setDebugEnabled(true)
//                .setOAuthConsumerKey("*********************")
//                .setOAuthConsumerSecret("******************************************")
//                .setOAuthAccessToken("**************************************************")
//                .setOAuthAccessTokenSecret("******************************************");
//        TwitterFactory tf = new TwitterFactory(cb.build());
//        Twitter twitter = tf.getInstance();
//
//    }
//
////    public SimpleProd(){
////
////        Properties properties = new Properties();
////        properties.put("bootstrap.servers", "localhost:9092");
////        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
////        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
////
////        // need to send message as producer record object
////
//////        ProducerRecord producerRecord = new ProducerRecord("test", "this is a message");
//////
//////        KafkaProducer kafkaProducer = new KafkaProducer(properties);
//////
//////        kafkaProducer.send(producerRecord);
//////        kafkaProducer.close();
////    }
//
//
//    public static void main(String[] args) {
//        System.out.println("this is going 2 b ze twitter Producee in le Java");
//
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("serializer.class", "kafka.serializer.StringEncoder");
//
//        ProducerConfig producerConfig = ProducerConfig(properties);
//
//        Producer<String, String> producer = new Producer<String, String>(producerConfig);
//
//        try {
//            TwitterStreamProducer.PushTwitterMessage(producer);
//        } catch (InterruptedException e) {
//            System.out.println(e);
//            }
//        }
//    }
//
