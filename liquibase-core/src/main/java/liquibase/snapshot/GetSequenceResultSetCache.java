package liquibase.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.structure.core.Schema;
import liquibase.util.StringUtils;

import java.sql.SQLException;
import java.util.*;

public class GetSequenceResultSetCache extends ResultSetCache.SingleResultSetExtractor {

    private Logger log = LogFactory.getInstance().getLog(GetSequenceResultSetCache.class.getSimpleName());

    private final Database database;
    private final String catalogName;
    private final String schemaName;
    private final String sequenceName;
    private final String allCatalogsScratchData;

    public GetSequenceResultSetCache(Database database, String allCatalogsScratchData, String catalogName, String schemaName, String sequenceName) {
        super(database);
        this.database = database;
        this.allCatalogsScratchData = allCatalogsScratchData;
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
    }

    @Override
    public ResultSetCache.RowData rowKeyParameters(CachedRow row) {
        return new ResultSetCache.RowData(catalogName, schemaName, database, row.getString("SEQUENCE_NAME"));
    }

    @Override
    public ResultSetCache.RowData wantedKeyParameters() {
        return new ResultSetCache.RowData(catalogName, schemaName, database, sequenceName);
    }

    @Override
    public boolean bulkContainsSchema(String schemaKey) {
        return false;
    }

    @Override
    public String getSchemaKey(CachedRow row) {
        return schemaName;
    }

    @Override
    boolean shouldBulkSelect(String schemaKey, ResultSetCache resultSetCache) {
        return sequenceName == null || allCatalogsScratchData != null || super.shouldBulkSelect(schemaKey, resultSetCache);
    }

    @Override
    public List<CachedRow> fastFetchQuery() throws SQLException, DatabaseException {
        String query = getSelectSequenceSql(new Schema(catalogName, schemaName), database, sequenceName);
        return executeAndExtract(query, database);
    }

    @Override
    public List<CachedRow> bulkFetchQuery() throws SQLException, DatabaseException {
        String query = getSelectSequenceSql(new Schema(catalogName, schemaName), database, null);
        return executeAndExtract(query, database);
    }

    protected String getSelectSequenceSql(Schema schema, Database database, String sequenceName) {
        String sql;
        if (database instanceof DB2Database) {
            if (database.getDatabaseProductName().startsWith("DB2 UDB for AS/400")) {
                sql = "SELECT SEQNAME AS SEQUENCE_NAME FROM QSYS2.SYSSEQUENCES WHERE SEQSCHEMA = '" + schema.getCatalogName() + "'";
            } else {
                sql = "SELECT SEQNAME AS SEQUENCE_NAME FROM SYSCAT.SEQUENCES WHERE SEQTYPE='S' AND SEQSCHEMA = '" + schema.getCatalogName() + "'";
            }
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND SEQNAME = '%s' ", sequenceName);
            }
        } else if (database instanceof Db2zDatabase) {
            sql = "SELECT NAME AS SEQUENCE_NAME, " +
                    "START AS START_VALUE, " +
                    "MINVALUE AS MIN_VALUE, " +
                    "MAXVALUE AS MAX_VALUE, " +
                    "CACHE AS CACHE_SIZE, " +
                    "INCREMENT AS INCREMENT_BY, " +
                    "CYCLE AS WILL_CYCLE, " +
                    "ORDER AS IS_ORDERED " +
                    "FROM SYSIBM.SYSSEQUENCES WHERE SEQTYPE = 'S' AND SCHEMA = '" + schema.getCatalogName() + "'";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND NAME = '%s' ", sequenceName);
            }
        } else if (database instanceof DerbyDatabase) {
            sql = "SELECT " +
                    "  seq.SEQUENCENAME AS SEQUENCE_NAME " +
                    "FROM " +
                    "  SYS.SYSSEQUENCES seq, " +
                    "  SYS.SYSSCHEMAS sch " +
                    "WHERE " +
                    "  sch.SCHEMANAME = '" + new CatalogAndSchema(null, schema.getName()).customize(database).getSchemaName() + "' AND " +
                    "  sch.SCHEMAID = seq.SCHEMAID";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND seq.SEQUENCENAME = '%s' ", sequenceName);
            }
        } else if (database instanceof FirebirdDatabase) {
            sql = "SELECT RDB$GENERATOR_NAME AS SEQUENCE_NAME FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND RDB$GENERATOR_NAME = '%s' ", sequenceName);
            }
        } else if (database instanceof H2Database) {
            sql = "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "' AND IS_GENERATED=FALSE";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND SEQUENCE_NAME = '%s' ", sequenceName);
            }
        } else if (database instanceof HsqlDatabase) {
            sql = "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "'";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND SEQUENCE_NAME = '%s' ", sequenceName);
            }
        } else if (database instanceof InformixDatabase) {
            sql = "SELECT tabname AS SEQUENCE_NAME FROM systables t, syssequences s WHERE s.tabid = t.tabid AND t.owner = '" + schema.getName() + "'";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND tabname = '%s' ", sequenceName);
            }
        } else if (database instanceof OracleDatabase) {
            sql = "SELECT SEQUENCE_NAME AS SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG AS WILL_CYCLE, ORDER_FLAG AS IS_ORDERED, LAST_NUMBER as START_VALUE, CACHE_SIZE FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = '" + schema.getCatalogName() + "'";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND SEQUENCE_NAME = '%s' ", sequenceName);
            }
        } else if (database instanceof PostgresDatabase) {
            int version = 9;
            try {
                version = database.getDatabaseMajorVersion();
            } catch (Exception ignore) {
                log.warning("Failed to retrieve database version: " + ignore);
            }

            if (version < 10) { // 'pg_sequence' view does not exists yet
                sql = "SELECT c.relname AS SEQUENCE_NAME FROM pg_class c " +
                        "join pg_namespace on c.relnamespace = pg_namespace.oid " +
                        "WHERE c.relkind='S' " +
                        "AND nspname = '" + schema.getName() + "' " +
                        "AND c.oid not in (select d.objid FROM pg_depend d where d.refobjsubid > 0)";
            } else {
                sql = "SELECT c.relname AS SEQUENCE_NAME, " +
                        "  s.seqmin AS MIN_VALUE, s.seqmax AS MAX_VALUE, s.seqincrement AS INCREMENT_BY, " +
                        "  s.seqcycle AS WILL_CYCLE, s.seqstart AS START_VALUE, s.seqcache AS CACHE_SIZE, " +
                        "  pg_catalog.format_type(s.seqtypid, NULL) AS SEQ_TYPE, " +
                        "  d.dep_owner AS OWNED_BY " +
                        "FROM pg_class c " +
                        "JOIN pg_namespace ns on c.relnamespace = ns.oid " +
                        "JOIN pg_sequence s on c.oid = s.seqrelid " +
                        "LEFT JOIN (" +
                        "  SELECT d.objid AS oid, " +
                        "    pg_catalog.quote_ident(nspname) || '.' ||" +
                        "    pg_catalog.quote_ident(relname) || '.' ||" +
                        "    pg_catalog.quote_ident(attname) AS dep_owner " +
                        "  FROM pg_class c " +
                        "  JOIN pg_catalog.pg_depend d ON c.oid = d.refobjid " +
                        "  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                        "  JOIN pg_catalog.pg_attribute a ON (" +
                        "    a.attrelid = c.oid " +
                        "    AND a.attnum = d.refobjsubid) " +
                        "  WHERE " +
                        "    c.relkind = 'S' " +
                        "    AND d.classid='pg_catalog.pg_class'::pg_catalog.regclass " +
                        "    AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass " +
                        "    AND d.deptype IN ('a', 'i') " +
                        ") AS d ON c.oid = d.oid " +
                        "WHERE c.relkind = 'S' " +
                        "AND ns.nspname = '" + schema.getName() + "' " +
                        "AND c.oid not in (select d.objid FROM pg_depend d where d.refobjsubid > 0)";
            }
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND c.relname = '%s' ", sequenceName);
            }
        } else if (database instanceof MSSQLDatabase) {
            sql = "SELECT SEQUENCE_NAME, " +
                    "cast(START_VALUE AS BIGINT) AS START_VALUE, " +
                    "cast(MINIMUM_VALUE AS BIGINT) AS MIN_VALUE, " +
                    "cast(MAXIMUM_VALUE AS BIGINT) AS MAX_VALUE, " +
                    "CAST(INCREMENT AS BIGINT) AS INCREMENT_BY, " +
                    "CYCLE_OPTION AS WILL_CYCLE " +
                    "FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "'";
            if (StringUtils.isNotEmpty(sequenceName)) {
                sql += String.format(" AND SEQUENCE_NAME = '%s' ", sequenceName);
            }
        } else {
            throw new UnexpectedLiquibaseException("Don't know how to query for sequences on " + database);
        }
        return sql;
    }


}
