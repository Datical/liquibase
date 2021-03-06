package liquibase.snapshot.jvm;

import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotIdService;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class SequenceSnapshotGenerator extends JdbcSnapshotGenerator {

    private Logger log = LogFactory.getInstance().getLog(SequenceSnapshotGenerator.class.getSimpleName());

    public SequenceSnapshotGenerator() {
        super(Sequence.class, new Class[]{Schema.class});
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!(foundObject instanceof Schema) || !snapshot.getDatabase().supportsSequences()) {
            return;
        }
        Schema schema = (Schema) foundObject;
        Database database = snapshot.getDatabase();

        //noinspection unchecked
        List<Map<String, ?>> sequences = ExecutorService.getInstance().getExecutor(database).queryForList(new RawSqlStatement(getSelectSequenceSql(schema, database)));

        if (sequences != null) {
            for (Map<String, ?> sequence : sequences) {
                schema.addDatabaseObject(mapToSequence(sequence, (Schema) foundObject, database));
            }
        }
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        if (example.getSnapshotId() != null) {
            return example;
        }
        Database database = snapshot.getDatabase();
        List<Map<String, ?>> sequences;
        if (database instanceof Db2zDatabase) {
            sequences = ExecutorService.getInstance()
                    .getExecutor(database)
                    .queryForList(new RawSqlStatement(getSelectSequenceSql(example.getSchema(), database)));
            return getSequences(example, database, sequences);
        } else {
            if (example.getAttribute("liquibase-complete", false)) { //need to go through "snapshotting" the object even if it was previously populated in addTo. Use the "liquibase-complete" attribute to track that it doesn't need to be fully snapshotted
                example.setSnapshotId(SnapshotIdService.getInstance().generateId());
                example.setAttribute("liquibase-complete", null);
                return example;
            }

            if (!database.supportsSequences()) {
                return null;
            }

            sequences = ExecutorService.getInstance()
                    .getExecutor(database)
                    .queryForList(new RawSqlStatement(getSelectSequenceSql(example.getSchema(), database)));
            DatabaseObject sequenceRow = getSequences(example, database, sequences);
            return sequenceRow;
        }
    }

    private DatabaseObject getSequences(DatabaseObject example, Database database, List<Map<String, ?>> sequences) {
        for (Map<String, ?> sequenceRow : sequences) {
            String name = cleanNameFromDatabase((String) sequenceRow.get("SEQUENCE_NAME"), database);
            if ((database.isCaseSensitive() && name.equals(example.getName()) || (!database.isCaseSensitive() && name.equalsIgnoreCase(example.getName())))) {
                return mapToSequence(sequenceRow, example.getSchema(), database);
            }
        }
        return null;
    }

    private Sequence mapToSequence(Map<String, ?> sequenceRow, Schema schema, Database database) {
        String name = cleanNameFromDatabase((String) sequenceRow.get("SEQUENCE_NAME"), database);
        Sequence seq = new Sequence();
        seq.setName(name);
        seq.setSchema(schema);
        seq.setStartValue(toBigInteger(sequenceRow.get("START_VALUE"), database));
        seq.setMinValue(toBigInteger(sequenceRow.get("MIN_VALUE"), database));
        seq.setMaxValue(toBigInteger(sequenceRow.get("MAX_VALUE"), database));
        seq.setCacheSize(toBigInteger(sequenceRow.get("CACHE_SIZE"), database));
        seq.setIncrementBy(toBigInteger(sequenceRow.get("INCREMENT_BY"), database));
        seq.setWillCycle(toBoolean(sequenceRow.get("WILL_CYCLE"), database));
        seq.setOrdered(toBoolean(sequenceRow.get("IS_ORDERED"), database));
        seq.setDataType((String) sequenceRow.get("SEQ_TYPE"));
        seq.setAttribute("liquibase-complete", true);

        return seq;
    }

    protected Boolean toBoolean(Object value, Database database) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        String valueAsString = value.toString();
        valueAsString = valueAsString.replace("'", "");
        if (valueAsString.equalsIgnoreCase("true")
                || valueAsString.equalsIgnoreCase("'true'")
                || valueAsString.equalsIgnoreCase("y")
                || valueAsString.equalsIgnoreCase("1")
                || valueAsString.equalsIgnoreCase("t")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    protected BigInteger toBigInteger(Object value, Database database) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }

        return new BigInteger(value.toString());
    }

    protected String getSelectSequenceSql(Schema schema, Database database) {
        if (database instanceof DB2Database) {
            if (database.getDatabaseProductName().startsWith("DB2 UDB for AS/400")) {
                return "SELECT SEQNAME AS SEQUENCE_NAME FROM QSYS2.SYSSEQUENCES WHERE SEQSCHEMA = '" + schema.getCatalogName() + "'";
            }
            return "SELECT SEQNAME AS SEQUENCE_NAME FROM SYSCAT.SEQUENCES WHERE SEQTYPE='S' AND SEQSCHEMA = '" + schema.getCatalogName() + "'";
        } else if (database instanceof Db2zDatabase) {
            return "SELECT NAME AS SEQUENCE_NAME, " +
                    "START AS START_VALUE, " +
                    "MINVALUE AS MIN_VALUE, " +
                    "MAXVALUE AS MAX_VALUE, " +
                    "CACHE AS CACHE_SIZE, " +
                    "INCREMENT AS INCREMENT_BY, " +
                    "CYCLE AS WILL_CYCLE, " +
                    "ORDER AS IS_ORDERED " +
                    "FROM SYSIBM.SYSSEQUENCES WHERE SEQTYPE = 'S' AND SCHEMA = '" + schema.getCatalogName() + "'";
        } else if (database instanceof DerbyDatabase) {
            return "SELECT " +
                    "  seq.SEQUENCENAME AS SEQUENCE_NAME " +
                    "FROM " +
                    "  SYS.SYSSEQUENCES seq, " +
                    "  SYS.SYSSCHEMAS sch " +
                    "WHERE " +
                    "  sch.SCHEMANAME = '" + new CatalogAndSchema(null, schema.getName()).customize(database).getSchemaName() + "' AND " +
                    "  sch.SCHEMAID = seq.SCHEMAID";
        } else if (database instanceof FirebirdDatabase) {
            return "SELECT RDB$GENERATOR_NAME AS SEQUENCE_NAME FROM RDB$GENERATORS WHERE RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0";
        } else if (database instanceof H2Database) {
            return "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "' AND IS_GENERATED=FALSE";
        } else if (database instanceof HsqlDatabase) {
            return "SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "'";
        } else if (database instanceof InformixDatabase) {
            return "SELECT tabname AS SEQUENCE_NAME FROM systables t, syssequences s WHERE s.tabid = t.tabid AND t.owner = '" + schema.getName() + "'";
        } else if (database instanceof OracleDatabase) {
            return "SELECT SEQUENCE_NAME AS SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG AS WILL_CYCLE, ORDER_FLAG AS IS_ORDERED, LAST_NUMBER as START_VALUE, CACHE_SIZE FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = '" + schema.getCatalogName() + "'";
        } else if (database instanceof PostgresDatabase) {
            int version = 9;
            try {
                version = database.getDatabaseMajorVersion();
            } catch (Exception ignore) {
                log.warning("Failed to retrieve database version: " + ignore);
            }
            if (version < 10) { // 'pg_sequence' view does not exists yet
                return "SELECT c.relname AS SEQUENCE_NAME FROM pg_class c " +
                        "join pg_namespace on c.relnamespace = pg_namespace.oid " +
                        "WHERE c.relkind='S' " +
                        "AND nspname = '" + schema.getName() + "' " +
                        "AND c.oid not in (select d.objid FROM pg_depend d where d.refobjsubid > 0)";
            } else {
                return "SELECT c.relname AS SEQUENCE_NAME, " +
                        "  s.seqmin AS MIN_VALUE, s.seqmax AS MAX_VALUE, s.seqincrement AS INCREMENT_BY, " +
                        "  s.seqcycle AS WILL_CYCLE, s.seqstart AS START_VALUE, s.seqcache AS CACHE_SIZE, " +
                        "  pg_catalog.format_type(s.seqtypid, NULL) AS SEQ_TYPE " +
                        "FROM pg_class c " +
                        "JOIN pg_namespace ns on c.relnamespace = ns.oid " +
                        "JOIN pg_sequence s on c.oid = s.seqrelid " +
                        "WHERE c.relkind = 'S' " +
                        "AND ns.nspname = '" + schema.getName() + "' " +
                        "AND c.oid not in (select d.objid FROM pg_depend d where d.refobjsubid > 0)";
            }
        } else if (database instanceof MSSQLDatabase) {
            return "SELECT SEQUENCE_NAME, " +
                    "cast(START_VALUE AS BIGINT) AS START_VALUE, " +
                    "cast(MINIMUM_VALUE AS BIGINT) AS MIN_VALUE, " +
                    "cast(MAXIMUM_VALUE AS BIGINT) AS MAX_VALUE, " +
                    "CAST(INCREMENT AS BIGINT) AS INCREMENT_BY, " +
                    "CYCLE_OPTION AS WILL_CYCLE " +
                    "FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = '" + schema.getName() + "'";
        } else {
            throw new UnexpectedLiquibaseException("Don't know how to query for sequences on " + database);
        }

    }
}
