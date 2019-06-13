package liquibase.snapshot.jvm;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.*;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;

import java.math.BigInteger;
import java.util.List;

public class SequenceSnapshotGenerator extends JdbcSnapshotGenerator {

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
        String catalogName = ((AbstractJdbcDatabase) database).getJdbcCatalogName(schema);
        String schemaName = ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema);

        try {
            JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();
            List<CachedRow> sequences = databaseMetaData.getSequences(catalogName, schemaName, null);
            if (sequences != null) {
                for (CachedRow sequence : sequences) {
                    schema.addDatabaseObject(toSequence(sequence, (Schema) foundObject, database));
                }
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        if (example.getSnapshotId() != null) {
            return example;
        }
        Database database = snapshot.getDatabase();
        Schema schema = example.getSchema();
        String catalogName = ((AbstractJdbcDatabase) database).getJdbcCatalogName(schema);
        String schemaName = ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema);

        try {
            JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaData();
            boolean caseSensitive = database.isCaseSensitive();
            List<CachedRow> sequences = databaseMetaData.getSequences(catalogName, schemaName, caseSensitive ? example.getName() : null); // for caseInsensitive DB do bulk
            if (database instanceof Db2zDatabase) {
                return getSequence(example, database, sequences, caseSensitive);
            } else {
                if (example.getAttribute("liquibase-complete", false)) { //need to go through "snapshotting" the object even if it was previously populated in addTo. Use the "liquibase-complete" attribute to track that it doesn't need to be fully snapshotted
                    example.setSnapshotId(SnapshotIdService.getInstance().generateId());
                    example.setAttribute("liquibase-complete", null);
                    return example;
                }

                if (!database.supportsSequences()) {
                    return null;
                }

                return getSequence(example, database, sequences, caseSensitive);
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    private DatabaseObject getSequence(DatabaseObject example, Database database, List<CachedRow> sequences, boolean caseSensitiveComparison) {
        if (sequences == null) {
            return null;
        }
        for (CachedRow sequenceRow : sequences) {
            String name = cleanNameFromDatabase((String) sequenceRow.get("SEQUENCE_NAME"), database);
            if ((caseSensitiveComparison && name.equals(example.getName()) || (!caseSensitiveComparison && name.equalsIgnoreCase(example.getName())))) {
                return toSequence(sequenceRow, example.getSchema(), database);
            }
        }
        return null;
    }

    private Sequence toSequence(CachedRow sequenceRow, Schema schema, Database database) {
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
        seq.setOwnedBy((String) sequenceRow.get("OWNED_BY"));
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

}
