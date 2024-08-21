# Liquibase 
<p align="center"><img src="https://github.com/liquibase/liquibase/blob/master/Liquibase.png" width="30%" height="30%"></p>

## Running Fossa Report

1. The Fossa Report Generation can be manually triggered from Datical/liquibase repository called `combine-fossa-report`. This triggers a run in the following array of repositories and return combining of report if all the reports are in csv format.
    repositories_triggered: ("DaticalDB-installer", "drivers", "ephemeral-database", "protoclub", "datical-sqlparser", "storedlogic", "AppDBA", "liquibase-bundle", "liquibase -Datical repo")
2. The report generation for datical-service is its own workflow file, in its own repsoitory
3. The reports are uploaded to a s3 bucket called with names `enterprise_report.csv and  datical-service.csv for now under bucket URI: https://liquibaseorg-origin.s3.amazonaws.com/enterprise_fossa_report/datical-service.csv and https://liquibaseorg-origin.s3.amazonaws.com/enterprise_fossa_report/enterprise_report.csv