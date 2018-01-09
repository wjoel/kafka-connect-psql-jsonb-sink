package com.wjoel.kafka_connect_psql_jsonb_sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.Collection;
import java.util.Map;

/**
 * Created by wjoel on 15/03/16.
 */
public class PsqlJsonbSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(PsqlJsonbSinkTask.class);

    private String connectString;
    private String user;
    private String password;
    private String table;
    private String keyColumn;
    private String valueColumn;
    private Connection connection;
    private PreparedStatement createTempTable;
    private PreparedStatement changeTempTableValueType;
    private PreparedStatement copyTempTable;
    private PreparedStatement dropTempTable;
    private String copyInSql;

    @Override
    public String version() { return new PsqlJsonbSinkConnector().version(); }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting with properties " + props.toString());
        connectString = props.get(PsqlJsonbSinkConnector.CONNECT_STRING_CONFIG);
        user = props.get(PsqlJsonbSinkConnector.USER_CONFIG);
        password = props.get(PsqlJsonbSinkConnector.PASSWORD_CONFIG);
        table = props.get(PsqlJsonbSinkConnector.TABLE_CONFIG);
        String keyColumnConfig = props.get(PsqlJsonbSinkConnector.KEY_COLUMN_CONFIG);
        if (keyColumnConfig != null && keyColumnConfig.length() > 0) {
            keyColumn = keyColumnConfig;
        } else {
            keyColumn = null;
        }
        valueColumn = props.get(PsqlJsonbSinkConnector.VALUE_COLUMN_CONFIG);
        String maybeKeyColumnComma = (keyColumn == null) ? "" : (keyColumn + ", ");
        try {
            connection = DriverManager.getConnection(connectString, user, password);
            String createTempTableSql = "CREATE TEMP TABLE temp0 AS SELECT * FROM " + table + " LIMIT 0";
            log.info("Creating table using: " + createTempTableSql);
            createTempTable = connection.prepareStatement(createTempTableSql);
            changeTempTableValueType = connection.prepareStatement(
                    "ALTER TABLE temp0 ALTER COLUMN " + valueColumn
                            + " TYPE json USING to_json(" + valueColumn + ")");
            String insertTempTableSql = "INSERT INTO " + table + " SELECT " + maybeKeyColumnComma
                + valueColumn + "::jsonb FROM temp0 ON CONFLICT DO NOTHING";
            log.info("Creating table using: " + insertTempTableSql);
            copyTempTable = connection.prepareStatement(insertTempTableSql);
            dropTempTable = connection.prepareStatement("DROP TABLE temp0");
        } catch (SQLException e) {
            throw new ConnectException("Failed to get database connection", e);
        }
        copyInSql = "COPY temp0 (" + maybeKeyColumnComma + valueColumn + ") FROM STDIN WITH BINARY";
        log.info("Copying from temp table using: " + copyInSql);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            return;
        }

        CopyManager cm;
        try {
            cm = ((PGConnection) connection).getCopyAPI();
        } catch (SQLException e) {
            log.error("Couldn't create CopyManager", e);
            throw new ConnectException(e);
        }

        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(bytes);

            data.writeBytes("PGCOPY\n"); // 12 bytes signature
            data.writeByte(255);
            data.writeBytes("\r\n");
            data.writeByte(0); // end of signature
            data.writeInt(0); // 4 bytes flag
            data.writeInt(0); // 4 bytes header extension

            for (SinkRecord record : sinkRecords) {
                // 2 bytes field count
                // <each field> 4 bytes field length, in bytes (-1 if NULL)
                //              data bytes
                if (keyColumn != null) {
                    data.writeShort((short) 2); // number of fields
                } else {
                    data.writeShort((short) 1);
                }

                if (keyColumn != null) {
                    if (record.key() == null) {
                        data.writeInt(-1); // -1 for NULL
                    } else {
                        String keyString = record.key().toString();
                        byte[] keyBytes = keyString.getBytes("UTF-8");
                        data.writeInt(keyBytes.length);
                        data.write(keyBytes);
                    }
                }
                if (record.value() == null) {
                    data.writeInt(-1);
                } else {
                    String valueString = record.value().toString();
                    byte[] valueBytes = valueString.getBytes("UTF-8");
                    data.writeInt(valueBytes.length);
                    data.write(valueBytes);
                }
            }
            data.writeShort((short) -1); // file trailer
            ByteArrayInputStream inputBytes = new ByteArrayInputStream(bytes.toByteArray());

            connection.setAutoCommit(false);
            createTempTable.execute();
            changeTempTableValueType.execute();
            cm.copyIn(copyInSql, inputBytes);
            copyTempTable.execute();
            dropTempTable.execute();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            log.error("Insertion failed", e);
            try {
                connection.rollback();
            } catch (SQLException rollbackException) {
                log.error("Rollback failed", rollbackException);
                throw new
                        ConnectException("Insertion failed, rollback failed", rollbackException);
            }
            throw new ConnectException("Insertion failed", e);
        } catch (UnsupportedEncodingException e) {
            log.error("Unsupported encoding");
            throw new ConnectException(e);
        } catch (IOException e) {
            log.error("IO error");
            throw new ConnectException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // nothing to do, because (I think) the connector manages the offset
        // TODO: make sure the connector actually does manage the offset
    }

    @Override
    public void stop() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            for (SQLException sqlException = e;
                 sqlException != null;
                 sqlException = sqlException.getNextException()) {
                log.error("SQL exception", e);
            }
            throw new ConnectException("Exception during close", e);
        }
    }
}
