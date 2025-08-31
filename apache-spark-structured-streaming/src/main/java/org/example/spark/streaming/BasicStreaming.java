package org.example.spark.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class BasicStreaming {
  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
       .builder()
       .master("local[3]")
       .appName("BasicStreaming")
       .getOrCreate();

    Dataset<Row> ds = spark
       .readStream()
       .format("socket")
       .option("host", "localhost")
       .option("port", 9091)
       .load();

    spark.udf().register("split0", (String str) -> str.split(",")[0], DataTypes.StringType);
    spark.udf().register("split1", (String str) -> str.split(",")[1], DataTypes.StringType);

    ds = ds.withColumn("level", callUDF("split0", col("value")))
       .withColumn("date", callUDF("split1", col("value")));

    ds = ds.drop(col("value"))
       .groupBy(col("level"))
       .count()
       .orderBy(desc("count"));

    StreamingQuery query = ds.writeStream()
       .outputMode("complete")
       .format("console")
       .start();

    query.awaitTermination();
  }
}
