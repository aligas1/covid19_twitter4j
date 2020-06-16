package com.training.kafkaSparkJava;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

import java.sql.*;

import static com.google.common.collect.ComparisonChain.start;
import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.streaming.OutputMode.Append;



public class ConsParsed {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaCons")
                .master("local[*]")
                .getOrCreate();

        // read schema from local

        StructType jsonSchema = spark.read()
                .json("/home/consultant/Desktop/javaTwitter.json")
                .schema();

        System.out.println(jsonSchema);

        // read kafka stream
        Dataset<Row> stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load();

        // create spark context to decrease logs... did not work sigh
        spark.sparkContext().setLogLevel("ERROR");

        // query spark sql dataframe
        Dataset<Row> dataDf = stream.selectExpr("CAST(value AS STRING) as json")
                .select(functions.from_json(functions.col("json"), jsonSchema).as("data"))
                .select("data.user.screen_name", "data.id", "data.lang", "data.created_at", "data.text");
//                .withColumnRenamed("user", "user_name");



        // print query to console
        StreamingQuery query = dataDf.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }


//
//        JDBCsink writer = new JDBCsink();


        //                .foreachBatch(
//                        new VoidFunction2<Dataset<Row>, Long>() {
//                            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
//                                // connect to Postgres
//                                Properties pgConnectionProperties = new Properties();
//                                pgConnectionProperties.put("user", "postgres");
//                                pgConnectionProperties.put("password", "1234");
//
//                                rowDataset.write()
//                                .mode(Append)
//                                .jdbc("jdbc:postgresql://localhost:5432/twitter", "public.table1", pgConnectionProperties);
//                            }
//                        }
//                )


//        dataDf.writeStream().foreachBatch(
//                new VoidFunction2<Dataset<Row>, Long>() {
//                    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
//                        rowDataset.write()
//                                .option("url", "jdbc:postgresql://localhost:5432/twitter")
//                                .option("dbtable", "table1")
//                                .option("user", "postgres")
//                                .option("password", 1234)
//                                .mode(SaveMode.Append)
//                                .save();
//                    }
//                }
//        ).start();


//        dataDf.write()
//                .format("jdbc")
//                .option("url", "jdbc:postgresql://localhost:5432/twitter")
//                .option("dbtable", "table1")
//                .option("user", "postgres")
//                .option("password", 1234)
//                .mode(SaveMode.Append)
//                .save();


//        try {
//            JDBCwrite.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }

        //  try to write dataDf to p




//
//
//        dataDf.createOrReplaceTempView("twitterDf");
//        Dataset<Row> sqlDf = spark.sql("SELECT * FROM df");
//
//        spark.table("twitterDf")
//                .write()
//                .mode(Append)
//                .jdbc("jdbc:postgresql://localhost:5432/twitter", "public.table1", pgConnectionProperties);

















    }



}
