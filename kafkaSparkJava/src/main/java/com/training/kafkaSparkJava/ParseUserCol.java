package com.training.kafkaSparkJava;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;


public class ParseUserCol {

    public static void main (String[] args) {

        System.out.println("this is a class to parse user col in JSON getting from API");


        SparkSession spark = SparkSession
                .builder()
                .appName("JavaCons")
                .master("local[*]")
                .getOrCreate();


        // spark session reads your json fle into a dataframe with .json()

        // reading in json file of sample kafka message
        Dataset<Row> json = spark.read().json("/home/consultant/Desktop/javaTwitter.json");

        // parse user schema
        json.select("user.screen_name").show();

        // schema of user, nested json column
        StructType jsonSchema = spark.read()
                .json("/home/consultant/Desktop/javaTwitter.json")
                .schema();

        json.show();

        // parse user schema
        json.select("user.screen_name").show();

    }

    //********************************************************************************
    //*****************************   user schema   **********************************
    //********************************************************************************
    /*
    StructField(user,
    StructType(StructField(contributors_enabled,BooleanType,true),
    StructField(created_at,StringType,true),
    StructField(default_profile,BooleanType,true),
    StructField(default_profile_image,BooleanType,true),
    StructField(description,StringType,true),
    StructField(favourites_count,LongType,true),
    StructField(follow_request_sent,StringType,true),
    StructField(followers_count,LongType,true),
    StructField(following,StringType,true),
    StructField(friends_count,LongType,true),
    StructField(geo_enabled,BooleanType,true),
    StructField(id,LongType,true),
    StructField(id_str,StringType,true),
    StructField(is_translator,BooleanType,true),
    StructField(lang,StringType,true),
    StructField(listed_count,LongType,true),
    StructField(location,StringType,true),
    StructField(name,StringType,true),
    StructField(notifications,StringType,true),
    StructField(profile_background_color,StringType,true),
    StructField(profile_background_image_url,StringType,true),
    StructField(profile_background_image_url_https,StringType,true),
    StructField(profile_background_tile,BooleanType,true),
    StructField(profile_banner_url,StringType,true),
    StructField(profile_image_url,StringType,true),
    StructField(profile_image_url_https,StringType,true),
    StructField(profile_link_color,StringType,true),
    StructField(profile_sidebar_border_color,StringType,true),
    StructField(profile_sidebar_fill_color,StringType,true),
    StructField(profile_text_color,StringType,true),
    StructField(profile_use_background_image,BooleanType,true),
    StructField(protected,BooleanType,true),
    StructField(screen_name,StringType,true),
    StructField(statuses_count,LongType,true),
    StructField(time_zone,StringType,true),
    StructField(translator_type,StringType,true),
    StructField(url,StringType,true),
    StructField(utc_offset,StringType,true),
    StructField(verified,BooleanType,true)),true) */

}
