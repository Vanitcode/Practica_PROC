package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichMovilWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesHourly(dataFrame: DataFrame): DataFrame

  def computeQuotaLimit(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, bytes_hourlyTable, quotaTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val movilMetadataDF = enrichMovilWithMetadata(antennaDF, metadataDF).cache()
    val aggByBytesHourlyDF = computeBytesHourly(movilMetadataDF)
    val aggQuotaDF = computeQuotaLimit(movilMetadataDF)


    writeToJdbc(aggByBytesHourlyDF, jdbcUri, bytes_hourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggQuotaDF, jdbcUri, quotaTable, jdbcUser, jdbcPassword)


    writeToStorage(antennaDF, storagePath)

    spark.close()
  }

}
