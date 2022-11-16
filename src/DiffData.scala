package com.bd.scala

import jv.{ SplitPartitions, DiffJobSession }
import com.datastax.spark.connector.cql.CassandraConnector
import org.slf4j.LoggerFactory
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object DiffData extends AbstractJob {

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  logger.info("Started Data Validation App")

  diffTable(sourceConnection, destinationConnection)

  exitSpark

  private def diffTable(sourceConnection: CassandraConnector, destinationConnection: CassandraConnector): Unit = {
    val partitions = SplitPartitions.getRandomSubPartitions(splitSize, minPartition, maxPartition, Integer.parseInt(coveragePercent))
    logger.info("PARAM Calculated -- Total Partitions: " + partitions.size())
    val parts = sContext.parallelize(partitions.toSeq, partitions.size)
    logger.info("Spark parallelize created : " + parts.count() + " parts!")

    parts.foreach(part => {
      sourceConnection.withSessionDo(sourceSession =>
        destinationConnection.withSessionDo(destinationSession =>
          DiffJobSession.getInstance(sourceSession, destinationSession)
            .getDataAndDiff(part.getMin, part.getMax)))
    })

    DiffJobSession.getInstance(null, null).printCounts("Job Final")
  }

}
