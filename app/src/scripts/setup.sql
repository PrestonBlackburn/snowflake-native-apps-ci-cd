-- ==========================================
-- Install Script 
-- ==========================================


-- create the application role that will be used to execute the app
create application role if not exists app_instance_role;
create or alter versioned schema app_instance_schema;

grant usage on schema app_instance_schema to application role app_instance_role;


-- Create Streamlit app
create or replace streamlit app_instance_schema.streamlit from '/libraries' main_file='streamlit.py';
grant usage on streamlit app_instance_schema.streamlit to application role app_instance_role;

-- Creating UDFs --

create or replace function app_instance_schema.predict_churn(monthly_charges float, total_charges float)
returns float
-- returns variant
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'pandas', 'scikit-learn')
--imports = ('/libraries/udf.py', '/libraries/models/regressor.pkl')
imports = ('/libraries/models/regressor.pkl')
--handler = 'udf.predict'
handler = 'predict'
as $$
import pandas as pd
import os
import sys
import pickle
from _snowflake import vectorized

@vectorized(input=pd.DataFrame)
def predict(df):
# def predict(num_1, num_2):
    # assumes monthly charges first, total chargest second
    df.columns = ["MONTHLY CHARGES", "TOTAL CHARGES"]
    

    import_dir = sys._xoptions.get("snowflake_import_directory")
    if import_dir:
        # with open(os.path.join(import_dir, 'libraries/models/regressor.pkl'), "rb") as model:
        with open(os.path.join(import_dir, 'regressor.pkl'), "rb") as model:
            regressor = pickle.load(model)


    # import_dir = sys._xoptions.get("snowflake_import_directory")
    # current_dir = os.listdir()
    # file_list = []
    # for file_obj in os.walk("."):
    #     try:
    #         file_list.append(file_obj)
    #
    #     except Exception as e:
    #         pass

    # return {"import_dir": import_dir,
    #         "current_dir": current_dir,
    #         "all_files": file_list}

    return regressor.predict(df)
$$;


-- Creating Sprocs --

-- Grant usage and permissions on objects --
grant usage on function app_instance_schema.predict_churn(float, float) to application role app_instance_role;


-- share data:
create or replace view app_instance_schema.telco_unlabeled as select * from shared_content_schema.telco_unlabeled;