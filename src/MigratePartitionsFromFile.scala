package com.bd.scala

import jv.{ SplitPartitions, CopyJobSession }
import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object MigratePartitionsFromFile extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started MigratePartitionsFromFile App")

  migrateTable(sourceConnection, destinationConnection)

  exitSpark

  private def migrateTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector): Unit = {
    val partitions = SplitPartitions.getSubPartitionsFromFile(splitSize)
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size)
    logger.info("Spark parallelize created : " + parts.count() + " parts!")

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          CopyJobSession.getInstance(sourceSession, destinationSession)
            .getDataAndInsert(part.getMin, part.getMax)))
    })
  }
}



