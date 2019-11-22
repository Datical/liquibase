package liquibase.diff.output.changelog.core;

import java.math.BigInteger;
import liquibase.change.Change;
import liquibase.change.core.CreateSequenceChange;
import liquibase.database.Database;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.AbstractChangeGenerator;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.diff.output.changelog.MissingObjectChangeGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Sequence;

public class MissingSequenceChangeGenerator extends AbstractChangeGenerator implements MissingObjectChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Sequence.class.isAssignableFrom(objectType)) {
            return PRIORITY_DEFAULT;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Class<? extends DatabaseObject>[] runAfterTypes() {
        return null;
    }

    @Override
    public Class<? extends DatabaseObject>[] runBeforeTypes() {
        return null;
    }

    @Override
    public Change[] fixMissing(DatabaseObject missingObject, DiffOutputControl control, Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        Sequence sequence = (Sequence) missingObject;

        CreateSequenceChange change = new CreateSequenceChange();
        change.setSequenceName(sequence.getName());
        if (control.getIncludeCatalog()) {
            change.setCatalogName(sequence.getSchema().getCatalogName());
        }
        if (control.getIncludeSchema()) {
            change.setSchemaName(sequence.getSchema().getName());
        }

        BigInteger incrementBy = sequence.getIncrementBy();
        Object incrementByDefault = comparisonDatabase.getDefaultValueForSequence("incrementBy", null);
        if (incrementBy != null && !(incrementBy.equals(incrementByDefault))) {
            change.setIncrementBy(incrementBy);
        }

        BigInteger maxValue = sequence.getMaxValue();
        Object lastNumber = sequence.getStartValue();
        Object maxValueDefaultMax = comparisonDatabase.getDefaultValueForSequence("maxValue", true);
        if (maxValue != null) {
            if((incrementBy != null) && (incrementBy.longValue() > 0) && !(maxValue.equals(maxValueDefaultMax))) {
                change.setMaxValue(maxValue);
            } else if((incrementBy != null) && (incrementBy.longValue() < 0) && !(maxValue.equals(lastNumber))) {
                change.setMaxValue(maxValue);
            }
        }

        BigInteger minValue = sequence.getMinValue();
        Object minValueDefaultMax = comparisonDatabase.getDefaultValueForSequence("minValue", true);
        Object minValueDefaultMin = comparisonDatabase.getDefaultValueForSequence("minValue", false);
        if (minValue != null) {
            if((incrementBy != null) && (incrementBy.longValue() > 0) && !(minValue.equals(minValueDefaultMax))) {
                change.setMinValue(minValue);
            } else if((incrementBy != null) && (incrementBy.longValue() < 0) && !(minValue.equals(minValueDefaultMin))) {
                change.setMinValue(minValue);
            }
        }

        Boolean cycle  = sequence.getWillCycle();
        Object cycleDefaultValue  = comparisonDatabase.getDefaultValueForSequence("cycle", null);
        if ((cycle != null) && !(cycle.equals(cycleDefaultValue))) {
            change.setCycle(cycle);
        }

        Boolean ordered  = sequence.getOrdered();
        Object orderedDefaultValue  = comparisonDatabase.getDefaultValueForSequence("ordered", null);
        if ((ordered != null) && !(ordered.equals(orderedDefaultValue))) {
            change.setOrdered(ordered);
        }

        BigInteger cacheSize = sequence.getCacheSize();
        Object cacheSizeDefaultValue = comparisonDatabase.getDefaultValueForSequence("cacheSize", null);
        if ((cacheSize != null) && !(cacheSize.equals(cacheSizeDefaultValue))) {
            change.setCacheSize(cacheSize);
        }

        change.setDataType(sequence.getDataType());
        change.setStartValue(sequence.getStartValue());

        return new Change[] { change };

    }
}
