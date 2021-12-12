package liquibase.statement.core;

import liquibase.statement.AbstractSqlStatement;

public class RawSqlStatement extends AbstractSqlStatement {

    private String sql;
    private String endDelimiter  = ";";
    private String outputResult;


    public RawSqlStatement(String sql) {
        this.sql = sql;
    }

    public RawSqlStatement(String sql, String endDelimiter) {
        this(sql);
        if (endDelimiter != null) {
            this.endDelimiter = endDelimiter;
        }
    }

    public String getSql() {
        return sql;
    }

    public String getEndDelimiter() {
        return endDelimiter.replace("\\r","\r").replace("\\n","\n");
    }

    public boolean isExecuted() {
        return isExecuted;
    }

    public void setExecuted(boolean isExecuted) {
        this.isExecuted = isExecuted;
    }

    @Override
    public String toString() {
        return sql;
    }

    public String getOutputResult() {
        return outputResult;
    }

    public void setOutputResult(String executionResult) {
        this.outputResult = executionResult;
    }

}
