package liquibase.snapshot.jvm;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.OfflineConnection;
import liquibase.database.core.*;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.logging.Logger;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.SqlUtil;
import liquibase.util.StringUtils;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnSnapshotGenerator extends JdbcSnapshotGenerator {

    protected static final String COLUMN_DEF_COL = "COLUMN_DEF";

    private Pattern postgresStringValuePattern = Pattern.compile("'(.*)'::[\\w .]+");
    private Pattern postgresNumberValuePattern = Pattern.compile("(\\d*)::[\\w .]+");


    public ColumnSnapshotGenerator() {
        super(Column.class, new Class[]{Table.class, View.class});
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Column column = (Column) example;
        Database database = snapshot.getDatabase();
        Relation relation = column.getRelation();

        if ((column.getComputed() != null) && (column.getComputed())) {
            return column;
        }

        Schema schema = relation.getSchema();
        try {

            if (column.getAttribute(LIQUIBASE_COMPLETE, false)) {
                column.setAttribute(LIQUIBASE_COMPLETE, null);
                return column;
            }

            String catalogName = ((AbstractJdbcDatabase) database).getJdbcCatalogName(schema);
            String schemaName = ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema);
            String tableName = relation.getName();
            String columnName = column.getName();

            JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData =
                    ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache();

            List<CachedRow> metaDataColumns = databaseMetaData.getColumns(catalogName,schemaName,tableName, columnName);
            List<CachedRow> metaDataNotNullConst = databaseMetaData.getNotNullConst(catalogName, schemaName, tableName);

            if (!metaDataColumns.isEmpty()) {
              CachedRow data = metaDataColumns.get(0);
              column = readColumn(data, relation, database);
              setAutoIncrementDetails(column, database, snapshot);

              populateValidateNullableIfNeeded(column, metaDataNotNullConst, database);
            }

            example.setAttribute(LIQUIBASE_COMPLETE, null);

            if (column == null && database instanceof PostgresDatabase && looksLikeFunction(example.getName())) {
                ((Column) example).setComputed(true);
                return example;
            }

            return column;
        } catch (DatabaseException|SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void populateValidateNullableIfNeeded(Column column, List<CachedRow> metaDataNotNullConst, Database database) {
        if(!(database instanceof OracleDatabase)) {
            return;
        }
        String name = column.getName();
        for (CachedRow cachedRow: metaDataNotNullConst) {
            Object columnNameObj = cachedRow.get("COLUMN_NAME");
            if (columnNameObj == null) {
                throw new AssertionError("Please check query to fetch data for notNullConst!. "
                    + "I didn't fetch needed data");
            }
            if (name.equalsIgnoreCase(columnNameObj.toString())){
                final String VALIDATE = "VALIDATED";
                Object validated = cachedRow.get(VALIDATE);
                if (validated== null) {
                    break;
                }
                // Oracle returns NULLABLE=Y for columns that have not null constraints that are not validated
                // we have to check the search_condition to verify if it is really nullable
                String searchCondition = cachedRow.getString("SEARCH_CONDITION");
                searchCondition = searchCondition == null ? "" : searchCondition.toUpperCase();
                String nullable = cachedRow.getString("NULLABLE");
                String constraintName = cachedRow.getString("CONSTRAINT_NAME");
                if ("NOT VALIDATED".equalsIgnoreCase(validated.toString())
                        && "Y".equalsIgnoreCase(nullable)
                        && searchCondition.matches("\"?\\w+\" IS NOT NULL")) {
                    // not validated not null constraint found
                    column.setNullable(false);
                    column.setShouldValidateNullable(false);
                }
                if (Boolean.FALSE.equals(column.isNullable()) && hasValidObjectName(constraintName)) {
                    column.setAttribute("notNullConstraintName", constraintName);
                }
            }
        }
    }

    private static boolean hasValidObjectName(String objectName) {
        if (StringUtils.isEmpty(objectName)) {
            return false;
        }
        return !objectName.startsWith("SYS_") && !objectName.startsWith("BIN$");
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        if (!snapshot.getSnapshotControl().shouldInclude(Column.class)) {
            return;
        }
        if (foundObject instanceof Relation) {
            Database database = snapshot.getDatabase();
            Relation relation = (Relation) foundObject;
            List<CachedRow> allColumnsMetadataRs;
            try {

                JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData =
                    ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache();

                Schema schema;

                schema = relation.getSchema();
                allColumnsMetadataRs = databaseMetaData.getColumns(
                        ((AbstractJdbcDatabase) database).getJdbcCatalogName(schema),
                        ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema),
                        relation.getName(),
                        null);
                List<CachedRow> metaDataNotNullConst = databaseMetaData.getNotNullConst(schema.getCatalogName(), schema.getName(), relation.getName());

                /*
                 * Microsoft SQL Server, SAP SQL Anywhere and probably other RDBMS guarantee non-duplicate
                 * ORDINAL_POSITIONs for the columns of a single table. But they do not guarantee there are no gaps
                 * in that integers (e.g. if columns have been deleted). So we need to check for that and renumber
                 * if needed.
                 */
                TreeMap<Integer, CachedRow> treeSet = new TreeMap<>();
                for (CachedRow row : allColumnsMetadataRs) {
                    treeSet.put(row.getInt("ORDINAL_POSITION"), row);
                }
                Logger log = LogService.getLog(getClass());

                // Now we can iterate through the sorted list and repair if needed.
                int currentOrdinal = 0;
                for (CachedRow row : treeSet.values()) {
                    currentOrdinal++;
                    int rsOrdinal = row.getInt("ORDINAL_POSITION");
                    if (rsOrdinal != currentOrdinal) {
                        log.debug(
                                LogType.LOG, String.format(
                                        "Repairing ORDINAL_POSITION with gaps for table=%s, column name=%s, " +
                                                "bad ordinal=%d, new ordinal=%d",
                                        relation.getName(),
                                        row.getString("COLUMN_NAME"),
                                        rsOrdinal,
                                        currentOrdinal
                                )
                        );
                        row.set("ORDINAL_POSITION", currentOrdinal);
                    }
                }

                // Iterate through all (repaired) rows and add the columns to our result.
                for (CachedRow row : allColumnsMetadataRs) {
                    Column column = readColumn(row, relation, database);
                    setAutoIncrementDetails(column, database, snapshot);
                    populateValidateNullableIfNeeded(column, metaDataNotNullConst, database);
                    column.setAttribute(LIQUIBASE_COMPLETE, !column.isNullable());
                    relation.getColumns().add(column);
                }
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

    }

    protected void setAutoIncrementDetails(Column column, Database database, DatabaseSnapshot snapshot) {
        if ((column.getAutoIncrementInformation() != null) &&
            (database instanceof MSSQLDatabase) &&
            (database
            .getConnection() != null) && !(database.getConnection() instanceof OfflineConnection)) {
            Map<String, Column.AutoIncrementInformation> autoIncrementColumns =
                (Map) snapshot.getScratchData("autoIncrementColumns");
            if (autoIncrementColumns == null) {
                autoIncrementColumns = new HashMap<>();
                Executor executor = ExecutorService.getInstance().getExecutor(database);
                try {
                    List<Map<String, ?>> rows = executor.queryForList(
                        new RawSqlStatement(
                            "SELECT object_schema_name(object_id) AS schema_name, " +
                                "object_name(object_id) AS table_name, name AS column_name, " +
                                "CAST(seed_value AS bigint) AS start_value, " +
                                "CAST(increment_value AS bigint) AS increment_by " +
                                "FROM sys.identity_columns"));
                    for (Map row : rows) {
                        String schemaName = (String) row.get("SCHEMA_NAME");
                        String tableName = (String) row.get("TABLE_NAME");
                        String columnName = (String) row.get("COLUMN_NAME");
                        Long startValue = (Long) row.get("START_VALUE");
                        Long incrementBy = (Long) row.get("INCREMENT_BY");

                        Column.AutoIncrementInformation info =
                            new Column.AutoIncrementInformation(startValue, incrementBy);
                        autoIncrementColumns.put(schemaName + "." + tableName + "." + columnName, info);
                    }
                    snapshot.setScratchData("autoIncrementColumns", autoIncrementColumns);
                } catch (DatabaseException e) {
                    LogService.getLog(getClass()).info(LogType.LOG, "Could not read identity information", e);
                }
            }
            if ((column.getRelation() != null) && (column.getSchema() != null)) {
                Column.AutoIncrementInformation autoIncrementInformation =
                    autoIncrementColumns.get(column.getSchema().getName() + "." + column.getRelation().getName()
                        + "." + column.getName());
                if (autoIncrementInformation != null) {
                    column.setAutoIncrementInformation(autoIncrementInformation);
                }
            }
        }
    }

    protected Column readColumn(CachedRow columnMetadataResultSet, Relation table, Database database)
        throws SQLException, DatabaseException {
        String rawTableName = (String) columnMetadataResultSet.get("TABLE_NAME");
        String rawColumnName = (String) columnMetadataResultSet.get("COLUMN_NAME");
        String rawSchemaName = StringUtils.trimToNull((String) columnMetadataResultSet.get("TABLE_SCHEM"));
        String rawCatalogName = StringUtils.trimToNull((String) columnMetadataResultSet.get("TABLE_CAT"));
        String remarks = StringUtils.trimToNull((String) columnMetadataResultSet.get("REMARKS"));
        if (remarks != null) {
            // Comes back escaped sometimes
            remarks = remarks.replace("''", "'");
        }
        Integer position = columnMetadataResultSet.getInt("ORDINAL_POSITION");


        Column column = new Column();
        column.setName(StringUtils.trimToNull(rawColumnName));
        column.setRelation(table);
        column.setRemarks(remarks);
        column.setOrder(position);

        if (columnMetadataResultSet.get("IS_FILESTREAM") != null && (Boolean) columnMetadataResultSet.get("IS_FILESTREAM")) {
            column.setAttribute("fileStream", true);
        }
        if (columnMetadataResultSet.get("IS_ROWGUIDCOL") != null && (Boolean) columnMetadataResultSet.get("IS_ROWGUIDCOL")) {
            column.setAttribute("rowGuid", true);
        }
        if (database instanceof OracleDatabase) {
            String nullable = columnMetadataResultSet.getString("NULLABLE");
            if ("Y".equals(nullable)) {
                column.setNullable(true);
            } else {
                column.setNullable(false);
            }
        } else {
            Integer nullable = columnMetadataResultSet.getInt("NULLABLE");
            if (nullable != null) {
                if (nullable == DatabaseMetaData.columnNoNulls) {
                    column.setNullable(false);
                } else if (nullable == DatabaseMetaData.columnNullable) {
                    column.setNullable(true);
                } else if (nullable == DatabaseMetaData.columnNullableUnknown) {
                    LogService.getLog(getClass()).info(LogType.LOG, "Unknown nullable state for column "
                            + column.toString() + ". Assuming nullable");
                    column.setNullable(true);
                }
            }
        }

        if (database.supportsAutoIncrement()) {
            if (table instanceof Table) {
                if (database instanceof OracleDatabase) {
                    Column.AutoIncrementInformation autoIncrementInfo = new Column.AutoIncrementInformation();
                    String data_default = StringUtils.trimToEmpty((String) columnMetadataResultSet.get("DATA_DEFAULT")).toLowerCase();
                    if (data_default.contains("iseq$$") && data_default.endsWith("nextval")) {
                        column.setAutoIncrementInformation(autoIncrementInfo);
                    }

                    Boolean isIdentityColumn = columnMetadataResultSet.yesNoToBoolean("IDENTITY_COLUMN");
                    if (Boolean.TRUE.equals(isIdentityColumn)) { // Oracle 12+
                        Boolean defaultOnNull = columnMetadataResultSet.yesNoToBoolean("DEFAULT_ON_NULL");
                        String generationType = columnMetadataResultSet.getString("GENERATION_TYPE");
                        autoIncrementInfo.setDefaultOnNull(defaultOnNull);
                        autoIncrementInfo.setGenerationType(generationType);

                        column.setAutoIncrementInformation(autoIncrementInfo);
                    }
                } else {
                    if (columnMetadataResultSet.containsColumn("IS_AUTOINCREMENT")) {
                        String isAutoincrement = (String) columnMetadataResultSet.get("IS_AUTOINCREMENT");
                        isAutoincrement = StringUtils.trimToNull(isAutoincrement);
                        if (isAutoincrement == null) {
                            column.setAutoIncrementInformation(null);
                        } else if (isAutoincrement.equals("YES")) {
                            column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                        } else if (isAutoincrement.equals("NO")) {
                            column.setAutoIncrementInformation(null);
                        } else if (isAutoincrement.equals("")) {
                            LogFactory.getLogger().info("Unknown auto increment state for column " + column.toString() + ". Assuming not auto increment");
                            column.setAutoIncrementInformation(null);
                        } else {
                            throw new UnexpectedLiquibaseException("Unknown is_autoincrement value: '" + isAutoincrement + "'");
                        }
                    } else {
                        //probably older version of java, need to select from the column to find out if it is auto-increment
                        String selectStatement;
                        if (database.getDatabaseProductName().startsWith("DB2 UDB for AS/400")) {
                            selectStatement = "select " + database.escapeColumnName(rawCatalogName, rawSchemaName, rawTableName, rawColumnName) + " from " + rawSchemaName + "." + rawTableName + " where 0=1";
                            LogService.getLog(getClass()).debug("rawCatalogName : <" + rawCatalogName + ">");
                            LogService.getLog(getClass()).debug("rawSchemaName : <" + rawSchemaName + ">");
                            LogService.getLog(getClass()).debug("rawTableName : <" + rawTableName + ">");
                            LogService.getLog(getClass()).debug("raw selectStatement : <" + selectStatement + ">");


                        } else {
                            selectStatement = "select " + database.escapeColumnName(rawCatalogName, rawSchemaName, rawTableName, rawColumnName) + " from " + database.escapeTableName(rawCatalogName, rawSchemaName, rawTableName) + " where 0=1";
                        }
                        LogService.getLog(getClass()).debug("Checking " + rawTableName + "." + rawCatalogName + " for auto-increment with SQL: '" + selectStatement + "'");
                        Connection underlyingConnection = ((JdbcConnection) database.getConnection()).getUnderlyingConnection();
                        Statement statement = null;
                        ResultSet columnSelectRS = null;

                        try {
                            statement = underlyingConnection.createStatement();
                            columnSelectRS = statement.executeQuery(selectStatement);
                            if (columnSelectRS.getMetaData().isAutoIncrement(1)) {
                                column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                            } else {
                                column.setAutoIncrementInformation(null);
                            }
                        } finally {
                            try {
                                if (statement != null) {
                                    statement.close();
                                }
                            } catch (SQLException ignore) {
                            }
                            if (columnSelectRS != null) {
                                columnSelectRS.close();
                            }
                        }
                    }
                }
            }
        }

        DataType type = readDataType(columnMetadataResultSet, column, database);
        column.setType(type);

        Object defaultValue = readDefaultValue(columnMetadataResultSet, column, database);

        // TODO Is uppercasing the potential function name always a good idea?
        // In theory, we could get a quoted function name (inprobable, but not impossible)
        if ((defaultValue != null) && (defaultValue instanceof DatabaseFunction) && ((DatabaseFunction) defaultValue)
            .getValue().matches("\\w+")) {
            defaultValue = new DatabaseFunction(((DatabaseFunction) defaultValue).getValue().toUpperCase());
        }
        column.setDefaultValue(defaultValue);
        column.setDefaultValueConstraintName(columnMetadataResultSet.getString("COLUMN_DEF_NAME"));

        return column;
    }

    /**
     * Processes metadata of a column, e.g. name, type and default value. We start with the result of the JDBC
     * {@link DatabaseMetaData}.getColumns() method. Depending on Database, additional columns might be present.
     *
     * @param columnMetadataResultSet the result from the JDBC getColumns() call for the column
     * @param column                  logical definition of the column (object form)
     * @param database                the database from which the column originates
     * @return a DataType object with detailed information about the type
     * @throws DatabaseException If an error occurs during processing (mostly caused by Exceptions in JDBC calls)
     */
    protected DataType readDataType(CachedRow columnMetadataResultSet, Column column, Database database) throws DatabaseException {

        if (database instanceof OracleDatabase) {
            String dataType = columnMetadataResultSet.getString("DATA_TYPE_NAME");
            dataType = dataType.replace("VARCHAR2", "VARCHAR");
            dataType = dataType.replace("NVARCHAR2", "NVARCHAR");

            DataType type = new DataType(dataType);
            type.setDataTypeId(columnMetadataResultSet.getInt("DATA_TYPE"));
            if (dataType.equalsIgnoreCase("NUMBER")) {
                type.setColumnSize(columnMetadataResultSet.getInt("DATA_PRECISION"));
//                if (type.getColumnSize() == null) {
//                    type.setColumnSize(38);
//                }
                type.setDecimalDigits(columnMetadataResultSet.getInt("DATA_SCALE"));
//                if (type.getDecimalDigits() == null) {
//                    type.setDecimalDigits(0);
//                }
//            type.setRadix(10);
            } else {
                if ("FLOAT".equalsIgnoreCase(dataType)) { //FLOAT [(precision)]
                    type.setColumnSize(columnMetadataResultSet.getInt("DATA_PRECISION"));
                } else {
                    type.setColumnSize(columnMetadataResultSet.getInt("DATA_LENGTH"));
                }

                boolean isTimeStampDataType = dataType.toUpperCase().contains("TIMESTAMP");

                if (isTimeStampDataType || dataType.equalsIgnoreCase("NCLOB") || dataType.equalsIgnoreCase("BLOB") || dataType.equalsIgnoreCase("CLOB")) {
                    type.setColumnSize(null);
                } else if (dataType.equalsIgnoreCase("NVARCHAR") || dataType.equalsIgnoreCase("NCHAR")) {
                    type.setColumnSize(columnMetadataResultSet.getInt("CHAR_LENGTH"));
                    type.setColumnSizeUnit(DataType.ColumnSizeUnit.CHAR);
                } else {
                    String charUsed = columnMetadataResultSet.getString("CHAR_USED");
                    DataType.ColumnSizeUnit unit = null;
                    if ("C".equals(charUsed)) {
                        unit = DataType.ColumnSizeUnit.CHAR;
                        type.setColumnSize(columnMetadataResultSet.getInt("CHAR_LENGTH"));
                    } else if ("B".equals(charUsed)) {
                        unit = DataType.ColumnSizeUnit.BYTE;
                    }
                    type.setColumnSizeUnit(unit);
                }
            }


            return type;
        }

        String columnTypeName = (String) columnMetadataResultSet.get("TYPE_NAME");

        if (database instanceof MSSQLDatabase) {
            if ("numeric() identity".equalsIgnoreCase(columnTypeName)) {
                columnTypeName = "numeric";
            } else if ("decimal() identity".equalsIgnoreCase(columnTypeName)) {
                columnTypeName = "decimal";
            } else if ("xml".equalsIgnoreCase(columnTypeName)) {
                columnMetadataResultSet.set("COLUMN_SIZE", null);
                columnMetadataResultSet.set("DECIMAL_DIGITS", null);
            } else if ("datetimeoffset".equalsIgnoreCase(columnTypeName)
                || "time".equalsIgnoreCase(columnTypeName)) {
                columnMetadataResultSet.set("COLUMN_SIZE", columnMetadataResultSet.getInt("DECIMAL_DIGITS"));
                columnMetadataResultSet.set("DECIMAL_DIGITS", null);
            }
        } else if (database instanceof PostgresDatabase) {
            columnTypeName = database.unescapeDataTypeName(columnTypeName);
        }

        if (database instanceof FirebirdDatabase) {
            if ("BLOB SUB_TYPE 0".equals(columnTypeName)) {
                columnTypeName = "BLOB";
            }
            if ("BLOB SUB_TYPE 1".equals(columnTypeName)) {
                columnTypeName = "CLOB";
            }
        }

        if ((database instanceof MySQLDatabase) && ("ENUM".equalsIgnoreCase(columnTypeName) || "SET".equalsIgnoreCase
            (columnTypeName))) {
            try {
                String boilerLength;
                if ("ENUM".equalsIgnoreCase(columnTypeName)) {
                    boilerLength = "7";
                } else  {
                    // SET
                    boilerLength = "6";
                }
                List<String> enumValues = ExecutorService.getInstance().getExecutor(database).queryForList(
                    new RawSqlStatement(
                        "SELECT DISTINCT SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(COLUMN_TYPE, " + boilerLength +
                            ", LENGTH(COLUMN_TYPE) - " + boilerLength +
                            " - 1 ), \"','\", 1 + units.i + tens.i * 10) , \"','\", -1)\n" +
                            "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                            "CROSS JOIN (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 " +
                            "UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) units\n" +
                            "CROSS JOIN (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 " +
                            "UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) tens\n" +
                            "WHERE TABLE_NAME = '" + column.getRelation().getName() + "' \n" +
                            "AND COLUMN_NAME = '" + column.getName() + "'"), String.class);
                String enumClause = "";
                for (String enumValue : enumValues) {
                    enumClause += "'" + enumValue + "', ";
                }
                enumClause = enumClause.replaceFirst(", $", "");
                return new DataType(columnTypeName + "(" + enumClause + ")");
            } catch (DatabaseException e) {
                LogService.getLog(getClass()).warning(LogType.LOG, "Error fetching enum values", e);
            }
        }

        DataType.ColumnSizeUnit columnSizeUnit = DataType.ColumnSizeUnit.BYTE;

        int dataType = columnMetadataResultSet.getInt("DATA_TYPE");
        Integer columnSize = null;
        Integer decimalDigits = null;

        if (!database.dataTypeIsNotModifiable(columnTypeName)) {
            // don't set size for types like int4, int8 etc
            columnSize = columnMetadataResultSet.getInt("COLUMN_SIZE");
            decimalDigits = columnMetadataResultSet.getInt("DECIMAL_DIGITS");
            if ((decimalDigits != null) && decimalDigits.equals(0)) {
                decimalDigits = null;
            }
        }

        Integer radix = columnMetadataResultSet.getInt("NUM_PREC_RADIX");

        Integer characterOctetLength = columnMetadataResultSet.getInt("CHAR_OCTET_LENGTH");

        if (database instanceof AbstractDb2Database) {
            String typeName = columnMetadataResultSet.getString("TYPE_NAME");
            if (("DBCLOB".equalsIgnoreCase(typeName) || "GRAPHIC".equalsIgnoreCase(typeName)
                || "VARGRAPHIC".equalsIgnoreCase(typeName)) &&(columnSize != null)) {
                //Stored as double length chars
                columnSize = columnSize / 2;
            }
            if ("TIMESTAMP".equalsIgnoreCase(columnTypeName) && (decimalDigits == null)) {
                // Actually a date
                columnTypeName = "DATE";
                dataType = Types.DATE;
            }
        }

        if ((database instanceof PostgresDatabase) && (columnSize != null) && columnSize.equals(Integer.MAX_VALUE)) {
            columnSize = null;
        }

        // For SAP (Sybase) SQL ANywhere, JDBC returns "LONG(2147483647) binary" (the number is 2^31-1)
        // but when creating a column, LONG BINARY must not have parameters.
        // The same applies to LONG(...) VARCHAR.
        if (database instanceof SybaseASADatabase
            && ("LONG BINARY".equalsIgnoreCase(columnTypeName) || "LONG VARCHAR".equalsIgnoreCase(columnTypeName))) {
            columnSize = null;
        }

        DataType type = new DataType(columnTypeName);
        type.setDataTypeId(dataType);

        /*
         * According to the description of DatabaseMetaData.getColumns, the content of the "COLUMN_SIZE" column is
         * pretty worthless for datetime/timestamp columns:
         *
         * "For datetime datatypes, this is the length in characters of the String representation
         * (assuming the maximum allowed precision of the fractional seconds component)."
         * In the case of TIMESTAMP columns, the information we are really looking for
         * (the fractional digits) is located in the column DECIMAL_DIGITS.
         */
        int jdbcType = columnMetadataResultSet.getInt("DATA_TYPE");

        // Java 8 compatibility notes: When upgrading this project to JDK8 and beyond, also execute this if-branch
        // if jdbcType is TIMESTAMP_WITH_TIMEZONE (does not exist yet in JDK7)
        if (jdbcType == Types.TIMESTAMP) {

            if (decimalDigits == null) {
                type.setColumnSize(null);
            } else {
                type.setColumnSize((decimalDigits != database.getDefaultFractionalDigitsForTimestamp()) ?
                    decimalDigits : null
                );
            }

            type.setDecimalDigits(null);
        } else {
            type.setColumnSize(columnSize);
            type.setDecimalDigits(decimalDigits);
        }
        type.setRadix(radix);
        type.setCharacterOctetLength(characterOctetLength);
        type.setColumnSizeUnit(columnSizeUnit);


        return type;
    }

    protected Object readDefaultValue(CachedRow columnMetadataResultSet, Column columnInfo, Database database) {
        if (database instanceof MSSQLDatabase) {
            Object defaultValue = columnMetadataResultSet.get(COLUMN_DEF_COL);

            if (((defaultValue != null) && (defaultValue instanceof String)) && ("(NULL)".equals(defaultValue))) {
                columnMetadataResultSet.set(COLUMN_DEF_COL, new DatabaseFunction("null"));
            }
        }

        if ((database instanceof OracleDatabase) && (columnMetadataResultSet.get(COLUMN_DEF_COL) == null)) {
            columnMetadataResultSet.set(COLUMN_DEF_COL, columnMetadataResultSet.get("DATA_DEFAULT"));

            if ((columnMetadataResultSet.get(COLUMN_DEF_COL) != null) && "NULL".equalsIgnoreCase((String)
                columnMetadataResultSet.get(COLUMN_DEF_COL))) {
                columnMetadataResultSet.set(COLUMN_DEF_COL, null);
            }

            Object columnDef = columnMetadataResultSet.get(COLUMN_DEF_COL);
            if ("CHAR".equalsIgnoreCase(columnInfo.getType().getTypeName()) && (columnDef instanceof String) && !
                ((String) columnDef).startsWith("'") && !((String) columnDef).endsWith("'")) {
                columnMetadataResultSet.set(COLUMN_DEF_COL, null);
                return new DatabaseFunction((String) columnDef);
            }

            if ("YES".equals(columnMetadataResultSet.get("VIRTUAL_COLUMN"))) {
                Object virtColumnDef = columnMetadataResultSet.get(COLUMN_DEF_COL);
                if ((virtColumnDef != null) && !"null".equals(virtColumnDef)) {
                    columnMetadataResultSet.set(COLUMN_DEF_COL, "GENERATED ALWAYS AS (" + virtColumnDef + ")");
                }
            }

            Object defaultValue = columnMetadataResultSet.get(COLUMN_DEF_COL);
            if ((defaultValue != null) && (defaultValue instanceof String)) {
                String lowerCaseDefaultValue = ((String) defaultValue).toLowerCase();
                if (lowerCaseDefaultValue.contains("iseq$$") && lowerCaseDefaultValue.endsWith(".nextval")) {
                    columnMetadataResultSet.set(COLUMN_DEF_COL, null);
                }

            }
        }

        if (database instanceof PostgresDatabase) {
            Object defaultValue = columnMetadataResultSet.get(COLUMN_DEF_COL);
            if ((defaultValue != null) && (defaultValue instanceof String)) {
                Matcher matcher = postgresStringValuePattern.matcher((String) defaultValue);
                if (matcher.matches()) {
                    defaultValue = matcher.group(1);
                } else {
                    matcher = postgresNumberValuePattern.matcher((String) defaultValue);
                    if (matcher.matches()) {
                        defaultValue = matcher.group(1);
                    }

                }
                columnMetadataResultSet.set(COLUMN_DEF_COL, defaultValue);
            }
        }

        if (
            (database instanceof AbstractDb2Database) &&
                ((columnMetadataResultSet.get(COLUMN_DEF_COL) != null) &&
                    "NULL".equalsIgnoreCase((String) columnMetadataResultSet.get(COLUMN_DEF_COL)))) {
            columnMetadataResultSet.set(COLUMN_DEF_COL, null);
        }

        return SqlUtil.parseValue(database, columnMetadataResultSet.get(COLUMN_DEF_COL), columnInfo.getType());
    }

    /**
     * {@link IndexSnapshotGenerator} fails to differentiate computed and non-computed column's for {@link PostgresDatabase}
     * assume that if COLUMN_NAME contains parentesised expression -- its function reference.
     * should handle cases like:
     * - ((name)::text)
     * - lower/upper((name)::text)
     * - (name)::text || '- concatenation example'
     */
    private boolean looksLikeFunction(String columnName) {
        return columnName.contains("(");
    }

    //START CODE FROM SQLITEDatabaseSnapshotGenerator

////    @Override
////    protected void readColumns(DatabaseSnapshot snapshot, String schema, DatabaseMetaData databaseMetaData) throws SQLException, DatabaseException {
////        Database database = snapshot.getDatabase();
////        updateListeners("Reading columns for " + database.toString() + " ...");
////
////        if (database instanceof SQLiteDatabase) {
////            // ...work around for SQLite
////            for (Table cur_table : snapshot.getTables()) {
////                Statement selectStatement = null;
////                ResultSet rs = null;
////                try {
////                    selectStatement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
////                    rs = databaseMetaData.getColumns(database.convertRequestedSchemaToCatalog(schema), database.convertRequestedSchemaToSchema(schema), cur_table.getName(), null);
////                    if (rs == null) {
////                        rs = databaseMetaData.getColumns(database.convertRequestedSchemaToCatalog(schema), database.convertRequestedSchemaToSchema(schema), cur_table.getName(), null);
////                    }
////                    while ((rs != null) && rs.next()) {
////                        readColumnInfo(snapshot, schema, rs);
////                    }
////                } finally {
////                    if (rs != null) {
////                        try {
////                            rs.close();
////                        } catch (SQLException ignored) {
////                        }
////                    }
////                    if (selectStatement != null) {
////                        selectStatement.close();
////                    }
////                }
////            }
////        } else {
////            // ...if it is no SQLite database
////            Statement selectStatement = null;
////            ResultSet rs = null;
////            try {
////                selectStatement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
////                rs = databaseMetaData.getColumns(database.convertRequestedSchemaToCatalog(schema), database.convertRequestedSchemaToSchema(schema), null, null);
////                while (rs.next()) {
////                    readColumnInfo(snapshot, schema, rs);
////                }
////            } finally {
////                if (rs != null) {
////                    try {
////                        rs.close();
////                    } catch (SQLException ignored) {
////                    }
////                }
////                if (selectStatement != null) {
////                    selectStatement.close();
////                }
////            }
////        }
////    }
//
////    private Column readColumnInfo(DatabaseSnapshot snapshot, String schema, ResultSet rs) throws SQLException, DatabaseException {
////        Database database = snapshot.getDatabase();
////        Column columnInfo = new Column();
////
////        String tableName = rs.getString("TABLE_NAME");
////        String columnName = rs.getString("COLUMN_NAME");
////        String schemaName = rs.getString("TABLE_SCHEM");
////        String catalogName = rs.getString("TABLE_CAT");
////
////        String upperCaseTableName = tableName.toUpperCase(Locale.ENGLISH);
////
////        if (database.isSystemTable(catalogName, schemaName, upperCaseTableName) ||
////                database.isLiquibaseTable(upperCaseTableName)) {
////            return null;
////        }
////
////        Table table = snapshot.getTable(tableName);
////        if (table == null) {
////            View view = snapshot.getView(tableName);
////            if (view == null) {
////                LogFactory.getLogger().debug("Could not find table or view " + tableName + " for column " + columnName);
////                return null;
////            } else {
////                columnInfo.setView(view);
////                view.getColumns().add(columnInfo);
////            }
////        } else {
////            columnInfo.setTable(table);
////            table.getColumns().add(columnInfo);
////        }
////
////        columnInfo.setName(columnName);
////        columnInfo.setDataType(rs.getInt("DATA_TYPE"));
////        columnInfo.setColumnSize(rs.getInt("COLUMN_SIZE"));
////        columnInfo.setDecimalDigits(rs.getInt("DECIMAL_POINTS"));
////        Object defaultValue = rs.getObject("COLUMN_DEF");
//////        try {
////            //todo columnInfo.setDefaultValue(TypeConverterFactory.getInstance().findTypeConverter(database).convertDatabaseValueToObject(defaultValue, columnInfo.getDataType(), columnInfo.getColumnSize(), columnInfo.getDecimalDigits(), database));
//////        } catch (ParseException e) {
//////            throw new DatabaseException(e);
//////        }
////
////        int nullable = rs.getInt("NULLABLE");
////        if (nullable == DatabaseMetaData.columnNoNulls) {
////            columnInfo.setNullable(false);
////        } else if (nullable == DatabaseMetaData.columnNullable) {
////            columnInfo.setNullable(true);
////        }
////
////        columnInfo.setPrimaryKey(snapshot.isPrimaryKey(columnInfo));
////        columnInfo.setAutoIncrement(isColumnAutoIncrement(database, schema, tableName, columnName));
////        String typeName = rs.getString("TYPE_NAME");
////        if (columnInfo.isAutoIncrement()) {
////            typeName += "{autoIncrement:true}";
////        }
////        columnInfo.setType(DataTypeFactory.getInstance().parse(typeName));
////
////        return columnInfo;
////    }
    //END CODE FROM SQLiteDatabaseSnapshotGenerator


    //method was from DerbyDatabaseSnapshotGenerator
//    @Override
//    protected Object readDefaultValue(Map<String, Object> columnMetadataResultSet, Column columnInfo, Database database) throws SQLException, DatabaseException {
//        Object val = columnMetadataResultSet.get("COLUMN_DEF");
//
//        if (val instanceof String && "GENERATED_BY_DEFAULT".equals(val)) {
//            return null;
//        }
//        return super.readDefaultValue(columnMetadataResultSet, columnInfo, database);
//    }


    //START CODE FROM MysqlDatabaseSnapshotGenerator


    //    @Override
//    protected Object readDefaultValue(Column columnInfo, ResultSet rs, Database database) throws SQLException, DatabaseException {
//            try {
//                Object tmpDefaultValue = columnInfo.getType().toLiquibaseType().sqlToObject(tableSchema.get(columnName).get(1), database);
//                // this just makes explicit the following implicit behavior defined in the mysql docs:
//                // "If an ENUM column is declared to permit NULL, the NULL value is a legal value for
//                // the column, and the default value is NULL. If an ENUM column is declared NOT NULL,
//                // its default value is the first element of the list of permitted values."
//                if (tmpDefaultValue == null && columnInfo.isNullable()) {
//                    columnInfo.setDefaultValue("NULL");
//                }
//                // column is NOT NULL, and this causes no "DEFAULT VALUE XXX" to be generated at all. per
//                // the above from MySQL docs, this will cause the first value in the enumeration to be the
//                // default.
//                else if (tmpDefaultValue == null) {
//                    columnInfo.setDefaultValue(null);
//                } else {
//                    columnInfo.setDefaultValue("'" + database.escapeStringForDatabase(tmpDefaultValue) + "'");
//                }
//            } catch (ParseException e) {
//                throw new DatabaseException(e);
//            }
//
//            // TEXT and BLOB column types always have null as default value
//        } else if (columnTypeName.toLowerCase().equals("text") || columnTypeName.toLowerCase().equals("blob")) {
//            columnInfo.setType(new DatabaseDataType(columnTypeName));
//            columnInfo.setDefaultValue(null);
//
//            // Parsing TIMESTAMP database.convertDatabaseValueToObject() produces incorrect results
//            // eg. for default value 0000-00-00 00:00:00 we have 0002-11-30T00:00:00.0 as parsing result
//        } else if (columnTypeName.toLowerCase().equals("timestamp") && !"CURRENT_TIMESTAMP".equals(tableSchema.get(columnName).get(1))) {
//            columnInfo.setType(new DatabaseDataType(columnTypeName));
//            columnInfo.setDefaultValue(tableSchema.get(columnName).get(1));
//        } else {
//            super.readDefaultValue(columnInfo, rs, database);
//        }
//
//    }

//    @Override
//    protected DatabaseDataType readDataType(ResultSet rs, Database database) throws SQLException {
//    	String columnTypeName = rs.getString("TYPE_NAME");
//        String columnName     = rs.getString("COLUMN_NAME");
//        String tableName      = rs.getString("TABLE_NAME");
//        String schemaName     = rs.getString("TABLE_CAT");
//
//        Map<String, List<String>> tableSchema = new HashMap<String, List<String>>();
//
//        if (!schemaCache.containsKey(tableName)) {
//
//            Statement selectStatement = null;
//            ResultSet rsColumnType = null;
//            try {
//                selectStatement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
//                rsColumnType = selectStatement.executeQuery("DESC "+database.escapeTableName(schemaName, tableName));
//
//                while(rsColumnType.next()) {
//                    List<String> colSchema = new ArrayList<String>();
//                    colSchema.add(rsColumnType.getString("Type"));
//                    colSchema.add(rsColumnType.getString("Default"));
//                    tableSchema.put(rsColumnType.getString("Field"), colSchema);
//                }
//            } finally {
//                if (rsColumnType != null) {
//                    try {
//                        rsColumnType.close();
//                    } catch (SQLException ignore) { }
//                }
//                if (selectStatement != null) {
//                    try {
//                        selectStatement.close();
//                    } catch (SQLException ignore) { }
//                }
//            }
//
//
//            schemaCache.put(tableName, tableSchema);
//
//        }
//
//        tableSchema = schemaCache.get(tableName);
//
//        // Parse ENUM and SET column types correctly
//        if (columnTypeName.toLowerCase().startsWith("enum") || columnTypeName.toLowerCase().startsWith("set")) {
//
//            DatabaseDataType dataType = new DatabaseDataType(tableSchema.get(columnName).get(0));
//        	try {
//                Object tmpDefaultValue = dataType.toLiquibaseType().sqlToObject(tableSchema.get(columnName).get(1), database);
//                // this just makes explicit the following implicit behavior defined in the mysql docs:
//                // "If an ENUM column is declared to permit NULL, the NULL value is a legal value for
//                // the column, and the default value is NULL. If an ENUM column is declared NOT NULL,
//                // its default value is the first element of the list of permitted values."
//                if (tmpDefaultValue == null && columnInfo.isNullable()) {
//                    columnInfo.setDefaultValue("NULL");
//                }
//                // column is NOT NULL, and this causes no "DEFAULT VALUE XXX" to be generated at all. per
//                // the above from MySQL docs, this will cause the first value in the enumeration to be the
//                // default.
//                else if (tmpDefaultValue == null) {
//                    columnInfo.setDefaultValue(null);
//                } else {
//                    columnInfo.setDefaultValue("'" + database.escapeStringForDatabase(tmpDefaultValue) + "'");
//                }
//        	} catch (ParseException e) {
//        		throw new DatabaseException(e);
//        	}
//
//        // TEXT and BLOB column types always have null as default value
//        } else if (columnTypeName.toLowerCase().equals("text") || columnTypeName.toLowerCase().equals("blob")) {
//        	columnInfo.setType(new DatabaseDataType(columnTypeName));
//        	columnInfo.setDefaultValue(null);
//
//        // Parsing TIMESTAMP database.convertDatabaseValueToObject() produces incorrect results
//        // eg. for default value 0000-00-00 00:00:00 we have 0002-11-30T00:00:00.0 as parsing result
//        } else if (columnTypeName.toLowerCase().equals("timestamp") && !"CURRENT_TIMESTAMP".equals(tableSchema.get(columnName).get(1))) {
//        	columnInfo.setType(new DatabaseDataType(columnTypeName));
//        	columnInfo.setDefaultValue(tableSchema.get(columnName).get(1));
//        } else {
//        	super.readDefaultValue(columnInfo, rs, database);
//        }
//    }


//    @Override
//    protected ForeignKeyInfo readForeignKey(ResultSet importedKeyMetadataResultSet) throws DatabaseException, SQLException {
//        ForeignKeyInfo fkinfo= super.readForeignKey(importedKeyMetadataResultSet);
//        //MySQL in reality doesn't has schemas. It has databases that can have relations like schemas.
//        fkinfo.setPkTableSchema(cleanObjectNameFromDatabase(importedKeyMetadataResultSet.getString("PKTABLE_CAT")));
//        fkinfo.setFkSchema(cleanObjectNameFromDatabase(importedKeyMetadataResultSet.getString("FKTABLE_CAT")));
//        return fkinfo;
//    }
//END CODE FROM MySQLDatabaseSNapshotGenerator

    //START CODE from InformixSnapshotGenerator
//    private static final Map<Integer, String> qualifiers = new HashMap<Integer, String>();
//
//    static {
//        qualifiers.put(0, "YEAR");
//        qualifiers.put(2, "MONTH");
//        qualifiers.put(4, "DAY");
//        qualifiers.put(6, "HOUR");
//        qualifiers.put(8, "MINUTE");
//        qualifiers.put(10, "SECOND");
//        qualifiers.put(11, "FRACTION(1)");
//        qualifiers.put(12, "FRACTION(2)");
//        qualifiers.put(13, "FRACTION(3)");
//        qualifiers.put(14, "FRACTION(4)");
//        qualifiers.put(15, "FRACTION(5)");
//    }
//    protected DataType readDataType(Map<String, Object> rs, Column column, Database database) throws SQLException {
//        // See http://publib.boulder.ibm.com/infocenter/idshelp/v115/topic/com.ibm.sqlr.doc/sqlr07.htm
//        String typeName = ((String) rs.get("TYPE_NAME")).toUpperCase();
//        if ("DATETIME".equals(typeName) || "INTERVAL".equals(typeName)) {
//            int collength = (Integer) rs.get("COLUMN_SIZE");
//            //int positions = collength / 256;
//            int firstQualifierType = (collength % 256) / 16;
//            int lastQualifierType = (collength % 256) % 16;
//            String type = "DATETIME".equals(typeName) ? "DATETIME" : "INTERVAL";
//            String firstQualifier = qualifiers.get(firstQualifierType);
//            String lastQualifier = qualifiers.get(lastQualifierType);
//            DataType dataTypeMetaData = new DataType(type + " " + firstQualifier + " TO " + lastQualifier);
//            dataTypeMetaData.setColumnSizeUnit(DataType.ColumnSizeUnit.BYTE);
//
//            return dataTypeMetaData;
//        } else {
//            return super.readDataType(rs, column, database);
//        }
//    }
    //END CODE FROM InformaixSnapshotGenerator

    //Code below was from OracleDatabaseSnapshotGenerator
    //    @Override
//    protected void readColumns(DatabaseSnapshot snapshot, String schema, DatabaseMetaData databaseMetaData) throws SQLException, DatabaseException {
//        findIntegerColumns(snapshot, schema);
//        super.readColumns(snapshot, schema, databaseMetaData);
//
//        /*
//          * Code Description:
//          * Finding all 'tablespace' attributes of column's PKs
//          * */
//        Database database = snapshot.getDatabase();
//        Statement statement = null;
//        ResultSet rs = null;
//        try {
//            statement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
//
//            // Setting default schema name. Needed for correct statement generation
//            if (schema == null)
//                schema = database.convertRequestedSchemaToSchema(schema);
//
//            String query = "select ui.tablespace_name TABLESPACE, ucc.table_name TABLE_NAME, ucc.column_name COLUMN_NAME FROM all_indexes ui , all_constraints uc , all_cons_columns ucc where uc.constraint_type = 'P' and ucc.constraint_name = uc.constraint_name and uc.index_name = ui.index_name and uc.owner = '" + schema + "' and ui.table_owner = '" + schema + "' and ucc.owner = '" + schema + "'";
//            rs = statement.executeQuery(query);
//
//            while (rs.next()) {
//                Column column = snapshot.getColumn(rs.getString("TABLE_NAME"), rs.getString("COLUMN_NAME"));
//                // setting up tablespace property to column, to configure it's PK-index
//                if (column == null) {
//                    continue; //probably a different schema
//                }
//                column.setTablespace(rs.getString("TABLESPACE"));
//            }
//        } finally {
//            if (rs != null) {
//                try {
//                    rs.close();
//                } catch (SQLException ignore) {
//                }
//            }
//            if (statement != null) {
//                try {
//                    statement.close();
//                } catch (SQLException ignore) {
//                }
//            }
//        }
//
//    }
//
//    /**
//     * Method finds all INTEGER columns in snapshot's database
//     *
//     * @param snapshot current database snapshot
//     * @return String list with names of all INTEGER columns
//     * @throws java.sql.SQLException execute statement error
//     */
//    private List<String> findIntegerColumns(DatabaseSnapshot snapshot, String schema) throws SQLException, DatabaseException {
//
//        Database database = snapshot.getDatabase();
//        // Setting default schema name. Needed for correct statement generation
//        if (schema == null) {
//            schema = database.convertRequestedSchemaToSchema(schema);
//        }
//        Statement statement = ((JdbcConnection) database.getConnection()).getUnderlyingConnection().createStatement();
//        ResultSet integerListRS = null;
//        // Finding all columns created as 'INTEGER'
//        try {
//            integerListRS = statement.executeQuery("select TABLE_NAME, COLUMN_NAME from all_tab_columns where data_precision is null and data_scale = 0 and data_type = 'NUMBER' and owner = '" + schema + "'");
//            while (integerListRS.next()) {
//                integerList.add(integerListRS.getString("TABLE_NAME") + "." + integerListRS.getString("COLUMN_NAME"));
//            }
//        } finally {
//            if (integerListRS != null) {
//                try {
//                    integerListRS.close();
//                } catch (SQLException ignore) {
//                }
//            }
//
//            if (statement != null) {
//                try {
//                    statement.close();
//                } catch (SQLException ignore) {
//                }
//            }
//        }
//
//
//        return integerList;
//    }
//
////    @Override
////    protected DatabaseDataType readDataType(ResultSet rs, Database database) throws SQLException {
////        if (integerList.contains(column.getTable().getName() + "." + column.getName())) {
////            column.setDataType(Types.INTEGER);
////        } else {
////            column.setDataType(rs.getInt("DATA_TYPE"));
////        }
////        column.setColumnSize(rs.getInt("COLUMN_SIZE"));
////        column.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
////
////        // Set true, if precision should be initialize
////        column.setInitPrecision(
////                !((column.getDataType() == Types.DECIMAL ||
////                        column.getDataType() == Types.NUMERIC ||
////                        column.getDataType() == Types.REAL) && rs.getString("DECIMAL_DIGITS") == null)
////        );
////    }
//
//
    ////    @Override
////    protected Object readDefaultValue(Column columnInfo, ResultSet rs, Database database) throws SQLException, DatabaseException {
////        super.readDefaultValue(columnInfo, rs, database);
////
////        // Exclusive setting for oracle INTEGER type
////        // Details:
////        // INTEGER means NUMBER type with 'data_precision IS NULL and scale = 0'
////        if (columnInfo.getDataType() == Types.INTEGER) {
////            columnInfo.setType(DataTypeFactory.getInstance().parse("INTEGER"));
////        }
////
////        String columnTypeName = rs.getString("TYPE_NAME");
////        if ("VARCHAR2".equals(columnTypeName)) {
////            int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
////            int columnSize = rs.getInt("COLUMN_SIZE");
////            if (columnSize == charOctetLength) {
////                columnInfo.setLengthSemantics(Column.ColumnSizeUnit.BYTE);
////            } else {
////                columnInfo.setLengthSemantics(Column.ColumnSizeUnit.CHAR);
////            }
////        }
////    }
}
