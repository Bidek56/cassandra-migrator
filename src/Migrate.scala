package com.bd.scala

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD

import jv.SplitPartitions
import jv.CopyJobSession
import jv.SplitPartitions.Partition

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Migrate extends AbstractJob {
  abstractLogger.info("Started Migration App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark()

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector): Unit = {
    val partitions: util.Collection[Partition] = SplitPartitions.getRandomSubPartitions(splitSize, minPartition, maxPartition, Integer.parseInt(coveragePercent))
    abstractLogger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts: RDD[SplitPartitions.Partition] = sContext.parallelize(partitions.toSeq, partitions.size)
    abstractLogger.info("Spark parallelize created : " + parts.count() + " parts!")

    parts.foreach(part => {
      abstractLogger.warn(s"part: $part")
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          CopyJobSession.getInstance(sourceSession, destinationSession, sc).getDataAndInsert(part.getMin, part.getMax)
        ))
    })
  }
}
