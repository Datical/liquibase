package liquibase.parser.core.xml;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.sdk.database.MockDatabase;
import liquibase.test.JUnitResourceAccessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class XMLChangeLogSAXParserTest {

    @Test
    public void testIgnoreDuplicateChangeSets() throws ChangeLogParseException, Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DatabaseChangeLog changeLog = xmlParser.parse("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/master.changelog.xml", 
            new ChangeLogParameters(), new JUnitResourceAccessor());

        final List<ChangeSet> changeSets = new ArrayList<ChangeSet>();

        new ChangeLogIterator(changeLog).run(new ChangeSetVisitor() {
            @Override
            public Direction getDirection() {
                return Direction.FORWARD;
            }

            @Override
            public void visit(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Set<ChangeSetFilterResult> filterResults) throws LiquibaseException {
                changeSets.add(changeSet);
            }
        }, new RuntimeEnvironment(new MockDatabase(), new Contexts(), new LabelExpression()));


        Assertions.assertEquals(8, changeSets.size());
        
        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog4.xml::1::testuser",
            changeSets.get(0).toString());
        
        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog4.xml::1::testuser",
            changeSets.get(1).toString());
        Assertions.assertEquals(1, changeSets.get(1).getContexts().getContexts().size());

        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog4.xml::1::testuser",
            changeSets.get(2).toString());
        Assertions.assertEquals(1, changeSets.get(2).getLabels().getLabels().size());

        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog4.xml::1::testuser",
            changeSets.get(3).toString());
        Assertions.assertEquals(2, changeSets.get(3).getLabels().getLabels().size());

        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog4.xml::1::testuser",
            changeSets.get(4).toString());
        Assertions.assertEquals(1, changeSets.get(4).getDbmsSet().size());

        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog1.xml::1::testuser",
            changeSets.get(5).toString());
        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog3.xml::1::testuser",
            changeSets.get(6).toString());
        Assertions.assertEquals("liquibase/parser/core/xml/ignoreDuplicatedChangeLogs/included.changelog2.xml::1::testuser",
            changeSets.get(7).toString());
    }

}