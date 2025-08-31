package org.example.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class DStreamKafkaStreaming {
  public static void main(String[] args) throws InterruptedException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("DStreamKafkaStreaming");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

    // kafka config

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "DStreamKafkaStreaming_Group_" + UUID.randomUUID().toString());
    kafkaParams.put("auto.offset.reset", "latest"); // start from current time - https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
    kafkaParams.put("enable.auto.commit", false); //https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
    Collection<String> topics = Arrays.asList("viewrecords");

    JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

    // 5 indicating the number of seconds watched as  this event is emitted every time after user watches the course for 5 seconds

    JavaPairDStream<Integer, String> javaPairDStream = stream.mapToPair(t -> new Tuple2<>(t.value(), 5))
       .reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(60))
       .mapToPair(item -> item.swap())
       .transformToPair(pairRdd -> pairRdd.sortByKey(false));

    javaPairDStream.print(50);

    jssc.start();
    jssc.awaitTermination();
  }
}
