package com.training.kafkaSparkJava;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.sql.*;

public class PostgresTest {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args){
        System.out.println("hello world");

        SparkSession spark = SparkSession
                .builder()
                .appName("SimpleJavaCons")
                .master("local[*]")
                .getOrCreate();


        // create instance of Bean class
        // JavaBeans are classes that encapsulate many objects intoa  single object (bean)
        Person person1 = new Person();
        person1.setName("John");
        person1.setAge(53);

        Person person2 = new Person();
        person2.setName("Asia");
        person2.setAge(30);

        // Encoder for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        // create a list of People instances
        List peopleList = new ArrayList();
        peopleList.add(person1);
        peopleList.add(person2);

        // create dataset
        Dataset<Person> javaBeanDS = spark.createDataset(peopleList,
                personEncoder);

        // convert to dataframe
        Dataset<Row> Df = spark.createDataFrame(peopleList, Person.class);
        System.out.println("Dataframe, Df, created");

        // connect to Postgres
        Properties pgConnectionProperties = new Properties();
        pgConnectionProperties.put("user", "postgres");
        pgConnectionProperties.put("password", "1234");


        // read table from Postgres
        Dataset<Row> EmployeeTable = spark.read()
                .jdbc("jdbc:postgresql://localhost:5432/test", "public.Employee", pgConnectionProperties);

        EmployeeTable.show();
        System.out.println("Reading table from Postgresql");

        // write data to table Postgresage

//        Dataset<Row> jdbcDf = spark.read()
//                .jdbc("jdbc:postgresql://localhost:5432/test", "public.writetrial", pgConnectionProperties);

        Df.createOrReplaceTempView("df");
        Dataset<Row> sqlDf = spark.sql("SELECT * FROM df");
        sqlDf.show();
        System.out.println("Sql Dataframe created");

//        spark.table("df")
                sqlDf
                .write()
                .mode(SaveMode.Append)
                .jdbc("jdbc:postgresql://localhost:5432/test", "public.writetrial", pgConnectionProperties);

    }

}
