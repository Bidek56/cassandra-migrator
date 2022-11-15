package com.bd.scala.jv;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.IntStream;

public class AbstractJobSession extends BaseJobSession {

    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    protected AbstractJobSession(CqlSession sourceSession, CqlSession astraSession) {
        this.sourceSession = sourceSession;
        this.astraSession = astraSession;

        // batchSize = Integer.valueOf(Util.getSparkPropOr(sc, "spark.batchSize", "1"));
        batchSize = 1; // Integer.valueOf(pr.get("spark.batchSize", "1"));

        printStatsAfter = 100000; // Integer.valueOf(pr.get("spark.printStatsAfter", "100000"));

        readLimiter = RateLimiter.create(20000); // RateLimiter.create(Integer.parseInt(pr.get("spark.readRateLimit", "20000")));
        writeLimiter = RateLimiter.create(40000); // RateLimiter.create(Integer.parseInt(pr.get("spark.writeRateLimit", "40000")));
        maxRetries = 10; // Integer.parseInt(pr.get("spark.maxRetries", "10"));

        sourceKeyspaceTable = "customer"; // pr.get( "spark.origin.keyspaceTable", "");
        astraKeyspaceTable =  "customer"; // pr.get( "spark.target.keyspaceTable", "");

//        String ttlColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.ttl.cols");
        String ttlColsStr = "6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65";
        if (null != ttlColsStr && ttlColsStr.trim().length() > 0) {
            for (String ttlCol : ttlColsStr.split(",")) {
                ttlCols.add(Integer.parseInt(ttlCol));
            }
        }

//        String writeTimestampColsStr = Util.getSparkPropOrEmpty(sc, "spark.query.writetime.cols");
        String writeTimestampColsStr = "6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65";
        if (null != writeTimestampColsStr && writeTimestampColsStr.trim().length() > 0) {
            for (String writeTimeStampCol : writeTimestampColsStr.split(",")) {
                writeTimeStampCols.add(Integer.parseInt(writeTimeStampCol));
            }
        }

        writeTimeStampFilter = false; //  Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.origin.writeTimeStampFilter", "false"));
        // batchsize set to 1 if there is a writeFilter
        if (writeTimeStampFilter) {
            batchSize = 1;
        }

        String minWriteTimeStampFilterStr = "0";  //                Util.getSparkPropOr(sc, "spark.origin.minWriteTimeStampFilter", "0");

        if (null != minWriteTimeStampFilterStr && minWriteTimeStampFilterStr.trim().length() > 1) {
            minWriteTimeStampFilter = Long.parseLong(minWriteTimeStampFilterStr);
        }
        String maxWriteTimeStampFilterStr = "9223372036854775807";
//                Util.getSparkPropOr(sc, "spark.origin.maxWriteTimeStampFilter", "0");
        if (null != maxWriteTimeStampFilterStr && maxWriteTimeStampFilterStr.trim().length() > 1) {
            maxWriteTimeStampFilter = Long.parseLong(maxWriteTimeStampFilterStr);
        }

        String customWriteTimeStr = "0";
//                Util.getSparkPropOr(sc, "spark.target.custom.writeTime", "0");
        if (null != customWriteTimeStr && customWriteTimeStr.trim().length() > 1 && StringUtils.isNumeric(customWriteTimeStr.trim())) {
            customWritetime = Long.parseLong(customWriteTimeStr);
        }

        logger.info("PARAM -- Write Batch Size: " + batchSize);
        logger.info("PARAM -- Source Keyspace Table: " + sourceKeyspaceTable);
        logger.info("PARAM -- Destination Keyspace Table: " + astraKeyspaceTable);
        logger.info("PARAM -- ReadRateLimit: " + readLimiter.getRate());
        logger.info("PARAM -- WriteRateLimit: " + writeLimiter.getRate());
        logger.info("PARAM -- TTLCols: " + ttlCols);
        logger.info("PARAM -- WriteTimestampFilterCols: " + writeTimeStampCols);
        logger.info("PARAM -- WriteTimestampFilter: " + writeTimeStampFilter);

        String selectCols = "id";

        //         Util.getSparkProp(sc, "spark.query.origin");
        String partionKey = "id"; // Util.getSparkProp(sc, "spark.query.origin.partitionKey");
        String sourceSelectCondition = ""; // Util.getSparkPropOrEmpty(sc, "spark.query.condition");

        final StringBuilder selectTTLWriteTimeCols = new StringBuilder();
        String[] allCols = selectCols.split(",");
        ttlCols.forEach(col -> selectTTLWriteTimeCols.append(",ttl(").append(allCols[col]).append(")"));
        writeTimeStampCols.forEach(col -> selectTTLWriteTimeCols.append(",writetime(").append(allCols[col]).append(")"));
        String fullSelectQuery = "select " + selectCols + selectTTLWriteTimeCols + " from " + sourceKeyspaceTable + " where token(" + partionKey.trim()
                + ") >= ? and token(" + partionKey.trim() + ") <= ?  " + sourceSelectCondition + " ALLOW FILTERING";
        sourceSelectStatement = sourceSession.prepare(fullSelectQuery);
        logger.info("PARAM -- Query used: " + fullSelectQuery);

        selectColTypes = getTypes( "9,2,2,13,0,13,2,4,4,4,4,1,3,1,3,1,3,1,3,2,2,10,13,0,4,3,0,0,0,0,0,4,10,0,4,0,12,4,3,3,0,0,13,0,0,1,9,4,13,0,0,10,2,2,2,10,3,0,1,2,4,13,9,2,2,2" ); // Util.getSparkProp(sc, "spark.query.types"));
        String idCols = "id"; // Util.getSparkPropOrEmpty(sc, "spark.query.target.id");

        idColTypes = selectColTypes.subList(0, idCols.split(",").length);

        String insertCols = "id"; // Util.getSparkPropOrEmpty(sc, "spark.query.target");
        if (null == insertCols || insertCols.trim().isEmpty()) {
            insertCols = selectCols;
        }
        StringBuilder insertBinds = new StringBuilder();
        for (String str : idCols.split(",")) {
            if (insertBinds.length() == 0) {
                insertBinds = new StringBuilder(str + "= ?");
            } else {
                insertBinds.append(" and ").append(str).append("= ?");
            }
        }
        astraSelectStatement = astraSession.prepare(
                "select " + insertCols + " from " + astraKeyspaceTable
                        + " where " + insertBinds);

        hasRandomPartitioner = false; // Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.origin.hasRandomPartitioner", "false"));
        isCounterTable = false; // Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.counterTable", "false"));
        if (isCounterTable) {
            String updateSelectMappingStr = "0"; // Util.getSparkPropOr(sc, "spark.counterTable.cql.index", "0");
            for (String updateSelectIndex : updateSelectMappingStr.split(",")) {
                updateSelectMapping.add(Integer.parseInt(updateSelectIndex));
            }

            String counterTableUpdate = ""; // Util.getSparkProp(sc, "spark.counterTable.cql");
            astraInsertStatement = astraSession.prepare(counterTableUpdate);
        } else {
            insertBinds = new StringBuilder();
            for (String ignored : insertCols.split(",")) {
                if (insertBinds.length() == 0) {
                    insertBinds.append("?");
                } else {
                    insertBinds.append(", ?");
                }
            }

            String fullInsertQuery = "insert into " + astraKeyspaceTable + " (" + insertCols + ") VALUES (" + insertBinds + ")";
            if (!ttlCols.isEmpty()) {
                fullInsertQuery += " USING TTL ?";
                if (!writeTimeStampCols.isEmpty()) {
                    fullInsertQuery += " AND TIMESTAMP ?";
                }
            } else if (!writeTimeStampCols.isEmpty()) {
                fullInsertQuery += " USING TIMESTAMP ?";
            }
            astraInsertStatement = astraSession.prepare(fullInsertQuery);
        }
    }

    public int getLargestTTL(Row sourceRow) {
        return IntStream.range(0, ttlCols.size())
                .map(i -> sourceRow.getInt(selectColTypes.size() + i)).max().getAsInt();
    }

    public long getLargestWriteTimeStamp(Row sourceRow) {
        return IntStream.range(0, writeTimeStampCols.size())
                .mapToLong(i -> sourceRow.getLong(selectColTypes.size() + ttlCols.size() + i)).max().getAsLong();
    }

    public BoundStatement selectFromAstra(PreparedStatement selectStatement, Row sourceRow) {
        BoundStatement boundSelectStatement = selectStatement.bind();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            boundSelectStatement = boundSelectStatement.set(index, getData(dataType, index, sourceRow),
                    dataType.typeClass);
        }

        return boundSelectStatement;
    }

}
