package io.keepcoding.spark.exercise.batch
import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MovilBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
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

  override def computeBytesHourly(dataFrame: DataFrame): DataFrame = {
    val antennaDF = dataFrame
      .select($"timestamp", $"bytes", $"antenna_id")
      .groupBy($"antenna_id".as("id"), window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("antenna_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")

    val userDF = dataFrame
      .select($"timestamp", $"bytes", $"id")
      .groupBy($"id", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("user_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")

    val appDF = dataFrame
      .select($"timestamp", $"bytes", $"app")
      .groupBy($"app".as("id"), window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("app_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")


    antennaDF.union(userDF).union(appDF)

  }


  override def computeQuotaLimit(dataFrame: DataFrame): DataFrame = {
    //user_quota_limit (email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP)
    dataFrame
      .select($"email", $"quota", $"timestamp", $"bytes")
      .groupBy($"email", $"quota", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("usage")
      )
      .select($"email", $"usage", $"quota", $"window.start".as("timestamp"))
      .where($"usage" > $"quota")


  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  def main(args: Array[String]): Unit = run(args)
}
