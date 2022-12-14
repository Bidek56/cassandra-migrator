package com.bd.scala.jv;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class DiffJobSession extends CopyJobSession {

    private static DiffJobSession diffJobSession;
    public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    protected Boolean autoCorrectMissing = false;
    protected Boolean autoCorrectMismatch = false;
    private final AtomicLong readCounter = new AtomicLong(0);
    private final AtomicLong mismatchCounter = new AtomicLong(0);
    private final AtomicLong missingCounter = new AtomicLong(0);
    private final AtomicLong correctedMissingCounter = new AtomicLong(0);
    private final AtomicLong correctedMismatchCounter = new AtomicLong(0);
    private final AtomicLong validCounter = new AtomicLong(0);
    private final AtomicLong skippedCounter = new AtomicLong(0);

    private DiffJobSession(CqlSession sourceSession, CqlSession astraSession) {
        super(sourceSession, astraSession);

        // autoCorrectMissing = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.missing", "false"));
        logger.info("PARAM -- Autocorrect Missing: " + autoCorrectMissing);

        // autoCorrectMismatch = Boolean.parseBoolean(Util.getSparkPropOr(sc, "spark.target.autocorrect.mismatch", "false"));
        logger.info("PARAM -- Autocorrect Mismatch: " + autoCorrectMismatch);
    }

    public static DiffJobSession getInstance(CqlSession sourceSession, CqlSession astraSession) {
        if (diffJobSession == null) {
            synchronized (DiffJobSession.class) {
                if (diffJobSession == null) {
                    diffJobSession = new DiffJobSession(sourceSession, astraSession);
                }
            }
        }

        return diffJobSession;
    }

    public void getDataAndDiff(BigInteger min, BigInteger max) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " Processing min: " + min + " max:" + max);
        int maxAttempts = maxRetries;
        for (int retryCount = 1; retryCount <= maxAttempts; retryCount++) {

            try {
                // cannot do batching if the writeFilter is greater than 0
                ResultSet resultSet = sourceSession.execute(
                        sourceSelectStatement.bind(hasRandomPartitioner ? min : min.longValueExact(), hasRandomPartitioner ? max : max.longValueExact()).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

                Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap = new HashMap<>();
                StreamSupport.stream(resultSet.spliterator(), false).forEach(srcRow -> {
                    readLimiter.acquire(1);
                    // do not process rows less than writeTimeStampFilter
                    if (!(writeTimeStampFilter && (getLargestWriteTimeStamp(srcRow) < minWriteTimeStampFilter
                            || getLargestWriteTimeStamp(srcRow) > maxWriteTimeStampFilter))) {
                        if (readCounter.incrementAndGet() % printStatsAfter == 0) {
                            printCounts("Current");
                        }

                        CompletionStage<AsyncResultSet> targetRowFuture = astraSession
                                .executeAsync(selectFromAstra(astraSelectStatement, srcRow));
                        srcToTargetRowMap.put(srcRow, targetRowFuture);
                        if (srcToTargetRowMap.size() > 1000) {
                            diffAndClear(srcToTargetRowMap);
                        }
                    } else {
                        readCounter.incrementAndGet();
                        skippedCounter.incrementAndGet();
                    }
                });
                diffAndClear(srcToTargetRowMap);

                printCounts("Final");

                retryCount = maxAttempts;
            } catch (Exception e) {
                logger.error("Error occurred retry#: " + retryCount, e);
                logger.error("Error with PartitionRange -- TreadID: " + Thread.currentThread().getId()
                        + " Processing min: " + min + " max:" + max + "    -- Retry# " + retryCount);
            }
        }

    }

    private void diffAndClear(Map<Row, CompletionStage<AsyncResultSet>> srcToTargetRowMap) {
        for (Row srcRow : srcToTargetRowMap.keySet()) {
            try {
                Row targetRow = srcToTargetRowMap.get(srcRow).toCompletableFuture().get().one();
                diff(srcRow, targetRow);
            } catch (Exception e) {
                logger.error("Could not perform diff for Key: " + getKey(srcRow), e);
            }
        }
        srcToTargetRowMap.clear();
    }

    public void printCounts(String finalStr) {
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Record Count: "
                + readCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Mismatch Count: "
                + mismatchCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Corrected Mismatch Count: "
                + correctedMismatchCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Missing Count: "
                + missingCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Corrected Missing Count: "
                + correctedMissingCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Valid Count: "
                + validCounter.get());
        logger.info("TreadID: " + Thread.currentThread().getId() + " " + finalStr + " Read Skipped Count: "
                + skippedCounter.get());
    }

    private void diff(Row sourceRow, Row astraRow) {
        if (astraRow == null) {
            missingCounter.incrementAndGet();
            logger.error("Missing target row found for key: " + getKey(sourceRow));
            //correct data

            if (autoCorrectMissing) {
                astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                correctedMissingCounter.incrementAndGet();
                logger.error("Inserted missing row in target: " + getKey(sourceRow));
            }

            return;
        }

        String diffData = isDifferent(sourceRow, astraRow);
        if (!diffData.isEmpty()) {
            mismatchCounter.incrementAndGet();
            logger.error("Mismatch row found for key: " + getKey(sourceRow) + " Mismatch: " + diffData);

            if (autoCorrectMismatch) {
                if (isCounterTable) {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, astraRow));
                } else {
                    astraSession.execute(bindInsert(astraInsertStatement, sourceRow, null));
                }
                correctedMismatchCounter.incrementAndGet();
                logger.error("Updated mismatch row in target: " + getKey(sourceRow));
            }

            return;
        }

        validCounter.incrementAndGet();
    }

    private String isDifferent(Row sourceRow, Row astraRow) {
        StringBuffer diffData = new StringBuffer();
        IntStream.range(0, selectColTypes.size()).parallel().forEach(index -> {
            MigrateDataType dataType = selectColTypes.get(index);
            Object source = getData(dataType, index, sourceRow);
            Object astra = getData(dataType, index, astraRow);

            boolean isDiff = dataType.diff(source, astra);
            if (isDiff) {
                diffData.append("(Index: ").append(index).append(" Origin: ").append(source).append(" Target: ").append(astra).append(" ) ");
            }
        });

        return diffData.toString();
    }

    private String getKey(Row sourceRow) {
        StringBuilder key = new StringBuilder();
        for (int index = 0; index < idColTypes.size(); index++) {
            MigrateDataType dataType = idColTypes.get(index);
            if (index == 0) {
                key.append(getData(dataType, index, sourceRow));
            } else {
                key.append(" %% ").append(getData(dataType, index, sourceRow));
            }
        }

        return key.toString();
    }

}
