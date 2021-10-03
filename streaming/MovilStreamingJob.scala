package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.Future

object MovilStreamingJob extends StreamingJob {


  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Movil Streaming")
    .getOrCreate()

  import spark.implicits._
  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val movilMessageSchema: StructType = ScalaReflection.schemaFor[MovilMessage].dataType.asInstanceOf[StructType]

    dataFrame
      .select(from_json(col("value").cast(StringType), movilMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
  }

  override def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichMovilWithMetadata(movilDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    movilDF.as("movil")
      .join(
        metadataDF.as("metadata"),
        $"movil.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  override def computeAntennaBytesCount(dataFrame: DataFrame): DataFrame = {
  // Tabla de referencia: bytes(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT)

    dataFrame
      .select($"timestamp", $"bytes", $"antenna_id")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"antenna_id".as("id"), window($"timestamp", "2 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("antenna_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")

  }
  override def computeUserBytesCount(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"bytes", $"id")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"id", window($"timestamp", "2 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("user_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")

  }

  override def computeAppBytesCount(dataFrame: DataFrame): DataFrame = {
   dataFrame
      .select($"timestamp", $"bytes", $"app")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"app".as("id"), window($"timestamp", "2 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("app_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")

  }
  override def writeToJdbcI(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }

  override def writeToJdbcII(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }

  override def writeToJdbcIII(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future{
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)


}
