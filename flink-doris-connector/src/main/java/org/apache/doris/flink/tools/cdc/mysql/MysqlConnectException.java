package org.apache.doris.flink.tools.cdc.mysql;

public class MysqlConnectException extends Exception {
    public MysqlConnectException(String s) {
        super(s);
    }

    public MysqlConnectException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public MysqlConnectException(Throwable throwable) {
        super(throwable);
    }
}
