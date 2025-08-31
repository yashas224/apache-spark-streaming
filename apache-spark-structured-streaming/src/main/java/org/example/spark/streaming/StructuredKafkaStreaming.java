package org.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredKafkaStreaming {
  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
       .builder()
       .master("local[*]")
       .appName("StructuredKafkaStreaming")
       .getOrCreate();

    Dataset<Row> df = spark
       .readStream()
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "viewrecords")
       .load();

    // to display contents of kafka message
//    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//
//    df.writeStream()
//       .outputMode("append")
//       .format("console")
//       .start()
//       .awaitTermination();

    df.createOrReplaceTempView("viewrecords_view");
    Dataset<Row> result = spark.sql("select window , cast (value as string) as course_name, sum(5) as total from viewrecords_view group by window(timestamp,'2 minutes') , course_name  ");
    StreamingQuery streamingQuery = result.writeStream().format("console").outputMode(OutputMode.Update()).option("truncate", false).start();

    streamingQuery.awaitTermination();
  }
}
