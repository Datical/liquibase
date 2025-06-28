package liquibase.changelog.filter

import liquibase.change.CheckSum
import liquibase.changelog.ChangeSet
import liquibase.changelog.RanChangeSet
import liquibase.database.Database
import liquibase.exception.DatabaseException
import liquibase.executor.Executor
import liquibase.executor.ExecutorService
import org.junit.jupiter.api.Test
import spock.lang.Specification

import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

public class ShouldRunChangeSetFilterTest extends Specification {

    static Database database

    public void setup() {
        database = Mock(Database.class)
    }

    public void accepts_noneRun() throws DatabaseException {
        when:
        database.getRanChangeSetList() >> new ArrayList<RanChangeSet>()

        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database)

        then:
        assertTrue(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "path/changelog", null, null, null)).isAccepted())
    }

    public void accepts() throws DatabaseException {
        when:
        given_a_database_with_two_executed_changesets()
        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database)

        then:
        assertFalse(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "path/changelog", null, null, null)).isAccepted(), "Already ran changeset should not be accepted")
        assertTrue(filter.accepts(new ChangeSet("1", "testAuthor", true, false, "path/changelog", null, null, null)).isAccepted(), "AlwaysRun changesets should always be accepted")
        assertTrue(filter.accepts(new ChangeSet("1", "testAuthor", false, true, "path/changelog", null, null, null)).isAccepted(), "RunOnChange changed changeset should be accepted")
        assertTrue(filter.accepts(new ChangeSet("3", "testAuthor", false, false, "path/changelog", null, null, null)).isAccepted(), "ChangeSet with different id should be accepted")
        assertTrue(filter.accepts(new ChangeSet("1", "otherAuthor", false, false, "path/changelog", null, null, null)).isAccepted(), "ChangeSet with different author should be accepted")
        assertTrue(filter.accepts(new ChangeSet("1", "testAuthor", false, false, "other/changelog", null, null, null)).isAccepted(), "ChangSet with different path should be accepted")
    }

    public void does_NOT_accept_current_changeset_with_classpath_prefix() throws DatabaseException {
        when:
        given_a_database_with_two_executed_changesets()
        ChangeSet changeSetWithClasspathPrefix = new ChangeSet("1", "testAuthor", false, false, "classpath:path/changelog", null, null, null)

        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database, true)

        then:
        assertFalse(filter.accepts(changeSetWithClasspathPrefix).isAccepted())
    }

    public void does_NOT_accept_current_changeset_when_inserted_changeset_has_classpath_prefix() throws DatabaseException {
        when:
        given_a_database_with_two_executed_changesets()
        ChangeSet changeSet = new ChangeSet("2", "testAuthor", false, false, "path/changelog", null, null, null)

        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database, true)

        then:
        assertFalse(filter.accepts(changeSet).isAccepted())
    }

    public void does_NOT_accept_current_changeset_when_both_have_classpath_prefix() throws DatabaseException {
        when:
        given_a_database_with_two_executed_changesets()
        ChangeSet changeSet = new ChangeSet("2", "testAuthor", false, false, "classpath:path/changelog", null, null, null)

        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database, true)

        then:
        assertFalse(filter.accepts(changeSet).isAccepted())
    }

//    public void should_decline_not_changed_changeset_when_has_run_on_change() throws DatabaseException {
//        when:
//        given_a_database_with_one_twice_executed_changeset()
//
//        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(database)
//
//        then:
//        assertFalse("RunOnChange not changed changeset should NOT be accepted", filter.accepts(new ChangeSet("1", "testAuthor", false, true, "path/changelog", null, null, null)).isAccepted())
//    }


    private Database given_a_database_with_two_executed_changesets() throws DatabaseException {
        ArrayList<RanChangeSet> ranChanges = new ArrayList<RanChangeSet>()
        RanChangeSet ranChangeSet1 = new RanChangeSet("path/changelog", "1", "testAuthor", CheckSum.parse("12345"), new Date(), null, null, null, null, null, null, null)
        ranChangeSet1.setOrderExecuted(1)
        ranChanges.add(ranChangeSet1)
        RanChangeSet ranChangeSet2 = new RanChangeSet("classpath:path/changelog", "2", "testAuthor", CheckSum.parse("12345"), new Date(), null, null, null, null, null, null, null)
        ranChangeSet2.setOrderExecuted(2)
        ranChanges.add(ranChangeSet2)

        return mock_database(ranChanges)
    }

    private Database mock_database(List<RanChangeSet> ranChanges) throws DatabaseException {
        database.getRanChangeSetList() >> ranChanges
        database.getDatabaseChangeLogTableName() >> "DATABASECHANGELOG"
        database.getDefaultSchemaName() >> null

        Executor template = Mock(Executor.class)
        template.update(_) >> 1

        ExecutorService.getInstance().setExecutor(database, template)
        return database
    }

    private Database given_a_database_with_one_twice_executed_changeset() throws DatabaseException {
        ArrayList<RanChangeSet> ranChanges = new ArrayList<RanChangeSet>()
        RanChangeSet ranChangeSet1 = new RanChangeSet("path/changelog", "1", "testAuthor", CheckSum.parse("not_matched_checksum"), new Date(), null, null, null, null, null, null, null)
        ranChangeSet1.setOrderExecuted(1)
        ranChanges.add(ranChangeSet1)
        RanChangeSet ranChangeSet2 = new RanChangeSet("path/changelog", "1", "testAuthor", CheckSum.parse("7:d41d8cd98f00b204e9800998ecf8427e"), new Date(), null, null, null, null, null, null, null)
        ranChangeSet2.setOrderExecuted(2)
        ranChanges.add(ranChangeSet2)

        return mock_database(ranChanges)
    }


    @Test
    public void should_decline_not_changed_changeset_when_has_run_on_change() throws DatabaseException {
        setup()
        ArrayList<RanChangeSet> ranChanges = new ArrayList<RanChangeSet>()
        ranChanges.add( new RanChangeSet("path/changelog", "1", "testAuthor", CheckSum.parse("8:d41d8cd98f00b204e9800998ecf8427e"), new Date(), null, null, null, null, null, null, null))
        Database db = mock_database(ranChanges)

        ShouldRunChangeSetFilter filter = new ShouldRunChangeSetFilter(db)

        assertFalse(filter.accepts(new ChangeSet("1", "testAuthor", false, true, "path/changelog", null, null, null)).isAccepted(), "RunOnChange not changed changeset should NOT be accepted")
    }
}