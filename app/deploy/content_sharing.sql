
--- might split something like this out from the deployment script

-- # this is for the data that will be referenced by streamlit

use database churn_prediction_package;
create schema 

--- for data sharing to run the udf ---
GRANT REFERENCE_USAGE ON DATABASE customer_db
  TO SHARE IN APPLICATION PACKAGE app_pkg;

-- # needs to be shared as a view
CREATE VIEW app_pkg.shared_schema.shared_view
  AS SELECT c1, c2, c3, c4
  FROM other_db.other_schema.other_table;


GRANT USAGE ON SCHEMA app_pkg.shared_schema
  TO SHARE IN APPLICATION PACKAGE app_pkg;
GRANT SELECT ON VIEW app_pkg.shared_schema.shared_view
  TO SHARE IN APPLICATION PACKAGE app_pkg;