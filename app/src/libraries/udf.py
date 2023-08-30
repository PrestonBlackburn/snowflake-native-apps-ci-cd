import pandas as pd
#from snowflake.snowpark.session import Session
#from snowflake.snowpark.functions import udf
#from snowflake.snowpark.row import Row
from snowflake.snowpark.types import PandasDataFrame, PandasSeries
import os
import sys
import pickle



# @udf(name = 'predict_churn', 
#     is_permanent = True,
#     stage_location="@MODELS",
#     replace=True,
#     packages = ['pandas',
#                    'scikit-learn',
#                    'snowflake-snowpark-python'],
#      session = session
#     )

def predict(df: PandasDataFrame[float, float]) -> PandasSeries[float]:
    # assumes monthly charges first, total chargest second
    df.columns = ["MONTHLY CHARGES", "TOTAL CHARGES"]
    
    import_dir = sys._xoptions.get("snowflake_import_directory")
    if import_dir:
        with open(os.path.join(import_dir, 'libraries/models/regressor.pkl'), "rb") as model:
            regressor = pickle.load(model)

    return regressor.predict(df)




