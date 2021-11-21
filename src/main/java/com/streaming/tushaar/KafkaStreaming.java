package com.streaming.tushaar;

import java.util.concurrent.TimeoutException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaStreaming {
	
	public static void main(String[] args) throws TimeoutException, StreamingQueryException {

		SparkConf sparkConf = new SparkConf();
		SparkSession spark = SparkSession.builder().master("local[*]").config(sparkConf).appName("kafka-streaming")
				.getOrCreate();

		Dataset<Row> df = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "tushaar").option("startingOffsets", "earliest").load();
		StreamingQuery query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream()
				.format("console").start();
		query.awaitTermination();
	}

}
