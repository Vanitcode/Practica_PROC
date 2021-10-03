package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class MovilMessage(bytes: Long, timestamp: Timestamp,  app: String, id: String, antenna_id: String)

/*

{"timestmap": 1600528288, "id": "550e8400-e29b-41d4-a716-446655440000", "antenna_id": "550e8400-1234-1234-a716-446655440000", "app": "SKYPE", "bytes": 100}
 */

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichMovilWithMetadata(movilDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeAntennaBytesCount(dataFrame: DataFrame): DataFrame
  def computeUserBytesCount(dataFrame: DataFrame): DataFrame
  def computeAppBytesCount(dataFrame: DataFrame): DataFrame

  def writeToJdbcI(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]
  def writeToJdbcII(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]
  def writeToJdbcIII(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val movilMetadataDF = enrichMovilWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
    val aggAntennaBytesDF = computeAntennaBytesCount(movilMetadataDF)
    val aggUserBytesDF = computeUserBytesCount(movilMetadataDF)
    val aggAppBytesDF = computeAppBytesCount(movilMetadataDF)
    val aggFutureI = writeToJdbcI(aggAntennaBytesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureII = writeToJdbcII(aggUserBytesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggFutureIII = writeToJdbcIII(aggAppBytesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(aggFutureI, storageFuture)), Duration.Inf)
    Await.result(Future.sequence(Seq(aggFutureII, storageFuture)), Duration.Inf)
    Await.result(Future.sequence(Seq(aggFutureIII, storageFuture)), Duration.Inf)

    spark.close()
  }

}
