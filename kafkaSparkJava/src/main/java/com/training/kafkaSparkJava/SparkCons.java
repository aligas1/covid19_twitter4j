package com.training.kafkaSparkJava;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import java.util.Arrays;
import java.util.Iterator;

public class SparkCons {

    // ask how to set logger level
//    static Logger logger = Logger.getLogger(SparkCons.class);
//    logger


    public static void main(String[] args) {
        System.out.println("hi");

        SparkSession spark = SparkSession
                .builder()
                .appName("SimpleJavaCons")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load();

        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("OFF");


        Dataset<Row> stream1 = stream.selectExpr("CAST(value AS STRING)");


        StreamingQuery query = stream1.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

}
