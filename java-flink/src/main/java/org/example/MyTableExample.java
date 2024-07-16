package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyTableExample {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set up the Flink table environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String create_source_table = "CREATE TABLE source_table (\n" +
                "    field1 STRING,\n" +
                "    field2 STRING\n" +  // Removed the comma here
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'input-topic',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9093',\n" +
                "    'properties.group.id' = 'my-group',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_source_table);

        String create_sink_table = "CREATE TABLE sink_table (\n" +
                "    field1 STRING,\n" +
                "    field2 STRING\n" +  // Removed the comma here
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'output-topic',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9093',\n" +
                "    'format' = 'json'\n" +
                ")";
        tableEnv.executeSql(create_sink_table);

        String insertQuery = "INSERT INTO sink_table " +
                "SELECT field1, field2 " +
                "FROM source_table " +
                "WHERE field2 = 'correct'";

        tableEnv.executeSql(insertQuery);
    }
}
