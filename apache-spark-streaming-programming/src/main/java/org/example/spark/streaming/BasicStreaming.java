package org.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class BasicStreaming {
  public static void main(String[] args) throws InterruptedException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

    JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 9091);

    JavaDStream<String> dStream = inputDStream.map(String::toString);
    dStream = dStream.map(s -> s.split(",")[0]);
    JavaPairDStream<String, Integer> javaPairDStream = dStream.mapToPair(s -> new Tuple2<>(s, 1));

    // per batch aggregation
//    javaPairDStream = javaPairDStream.reduceByKey((n1, n2) -> n1 + n2);

    // aggregation based on window size
    javaPairDStream = javaPairDStream.reduceByKeyAndWindow((n1, n2) -> n1 + n2, Durations.minutes(2));

    javaPairDStream.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
