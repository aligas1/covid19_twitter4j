package com.training.twitterKafkaProd;

import twitter4j.*;
import java.util.List;

public class GetTweets {

    public static void main(String[] args) throws TwitterException {

        System.out.println("gonna get some tweets");

        TwitterFactory tf = new TwitterFactory();
        Twitter twitter = tf.getInstance();

        // prints tweets from timeline
//        List<Status> status = twitter.getHomeTimeline();
//
//        for (Status s:status) {
//            System.out.println(s.getUser().getScreenName() + ": " + s.getText());
//        }

        // streaming tweets
        FilterQuery query;

        TwitterStream twitterStream = new TwitterStreamFactory()
                .getInstance()
                .addListener(new StatusListener() {
                    Integer count = 1;
                    public void onStatus(Status status) {

                        String stringMessage = status.toString();

                        // print status json and increment counter
                        System.out.println(status);
                        System.out.println("");
                        System.out.println(count);
                        count += 1;
                        System.out.println(stringMessage.getClass());
                        System.out.println("");
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
