package liquibase.license;

import liquibase.exception.ValidationErrors;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.changelog.ChangeSet;

/**
 *
 * This class provides a static method for verifying licenses
 *
 */
public class LicenseServiceUtils {
    private static Logger LOG = LogFactory.getLogger();

    public static ValidationErrors checkForValidLicense(String licenseType, ChangeSet changeSet) {
      ValidationErrors validationErrors = new ValidationErrors();
      return validationErrors;
    }
}
