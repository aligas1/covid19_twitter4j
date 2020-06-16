//package com.training.kafkaSparkJava;
//
//import org.apache.spark.sql.ForeachWriter;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//public class JDBCsink extends ForeachWriter{
//
//    PostgresqlConnectionFactory connecctionFactory = new PostgresqlConenctionFactory(PostgresqlConnectionConfiguration.builder()
//    .host()
//    .database()
//    .username()
//    .password().build())
//
//    Connection connection = null;
//    Statement statement = null;
//
//    public boolean open(long partitionId, long epochId) {
//        try {
//            Class.forName("org.postgresql.Driver");
//            try {
//                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/twitter", "postgres", "1234");
//                connection.setAutoCommit(true);
//                statement = connection.prepareStatement("insert into table1 values ()");
//
//            } catch (SQLException throwables) {
//                throwables.printStackTrace();
//            }
//
//
//
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//
//        return true;
//    }
//
//    public void process(Object value) {
//
//                    try {
//                statement.executeUpdate("INSERT INTO public.table1 VALUES (SELECT * FROM dataDf)");
//            } catch (SQLException throwables) {
//                throwables.printStackTrace();
//            }
//
//    }
//
//    public void close(Throwable errorOrNull) {
//
//        try {
//            connection.close();
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        }
//
//    }
//}
