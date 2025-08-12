package liquibase.sqlgenerator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeneratorLevelTest {

    @SuppressWarnings("unchecked")
	@Test
    public void checkLevelsAndNaming() {
        for (SqlGenerator generator : SqlGeneratorFactory.getInstance().getGenerators()) {
            int specializationlevel = generator.getPriority();
            String className = generator.getClass().getName();
            if (className.contains(".ext.")) {
                //not one to test, a test class
            } else if (className.endsWith("CreateTableGeneratorInformix")) {
                //had to change level for some reason
            } else if (className.endsWith("Generator")) {
                assertEquals(SqlGenerator.PRIORITY_DEFAULT, specializationlevel, "Incorrect level/naming convention for "+ className);
            } else {
                assertEquals(SqlGenerator.PRIORITY_DATABASE, specializationlevel, "Incorrect level/naming convention for "+ className);
            }
        }
    }
}
