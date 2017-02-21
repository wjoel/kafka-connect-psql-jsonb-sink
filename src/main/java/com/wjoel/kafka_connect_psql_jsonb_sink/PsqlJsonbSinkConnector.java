package com.wjoel.kafka_connect_psql_jsonb_sink;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wjoel on 15/03/16.
 */
public class PsqlJsonbSinkConnector extends SinkConnector {
    public static final String CONNECT_STRING_CONFIG = "db.connectString";
    public static final String USER_CONFIG = "db.user";
    public static final String PASSWORD_CONFIG = "db.password";
    public static final String TABLE_CONFIG = "db.table";
    public static final String KEY_COLUMN_CONFIG = "db.table.key.column";
    public static final String VALUE_COLUMN_CONFIG = "db.table.value.column";

    private static final Logger log = LoggerFactory.getLogger(PsqlJsonbSinkConnector.class);

    private String connectString;
    private String user;
    private String password;
    private String table;
    private String keyColumn;
    private String valueColumn;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (java.lang.ClassNotFoundException e) {
            log.error("Failed to load PostgreSQL driver");
            throw new ConnectException("Failed to load PostgreSQL driver");
        }
        connectString = props.get(CONNECT_STRING_CONFIG);
        user = props.get(USER_CONFIG);
        password = props.get(PASSWORD_CONFIG);
        table = props.get(TABLE_CONFIG);
        keyColumn = props.get(KEY_COLUMN_CONFIG);
        valueColumn = props.get(VALUE_COLUMN_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PsqlJsonbSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            if (connectString != null && table != null) {
                config.put(CONNECT_STRING_CONFIG, connectString);
                config.put(USER_CONFIG, user);
                config.put(PASSWORD_CONFIG, password);
                config.put(TABLE_CONFIG, table);
                config.put(KEY_COLUMN_CONFIG, keyColumn);
                config.put(VALUE_COLUMN_CONFIG, valueColumn);
            }
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do, we have no background monitoring.
    }
}
