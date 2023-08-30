# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.row import Row

import pandas as pd

# for local testing
import configparser

try:
    session = get_active_session()

    # specify environment
    env = "app"

    import snowflake.permissions as permission

except:
    # create a local session if there is no default session
    # also need to create procs beforehand
    config = configparser.ConfigParser()
    config.read('sf_account.config')
    session = Session.builder.configs(dict(config['DEFAULT'])).create() 
    
    # specify environment
    env = "local"

def snowpark_query(session, sql, non_select=False, dry=False, collect=False):
    """
    non-select queries include things like:
     "show databases;"

    """

    if non_select:
        # apply the Row function "as_dict" to all rows then conver to pandas
        row_objs = session.sql(sql).collect()

        dict_array = list(map(Row.as_dict, row_objs))

        df = pd.DataFrame.from_records(dict_array)

        return df

    else:
        df = session.sql(sql).to_pandas()

        return df


st.title("Churn Prediction")




# select candidate table
st.header("Predict Churn on Dataset")

if env == "app":
    telco_reference_associations = permission.get_reference_associations("telco_unlabeled")
    if len(telco_reference_associations) == 0:
        permission.request_reference("telco_unlabeled")
        exit(0)



if env == "local":
    table = "customer_db.churn.telco_unlabeled"
    udf_prefix = "customer_db.churn."

if env == "app":
    #table = "customer_db.churn.telco_unlabeled"
    table = "dev_churn_prediction_package.shared_content_schema.telco_unlabeled"
    # table = "reference('telco_unlabeled')"
    udf_prefix = ""




if session:


    predict_sql = f"""
    SELECT
        CUSTOMERID, 
        "MONTHLY CHARGES", 
        "TOTAL CHARGES",
        {udf_prefix}PREDICT_CHURN("MONTHLY CHARGES", "TOTAL CHARGES") as churn_prediction
    from {table}
    where churn_prediction > 0.1
    limit 10
    """

    prediction_df = snowpark_query(session, predict_sql, non_select=True)

    st.dataframe(prediction_df)



# "What If" analysis

st.header("What If Analysis")

with st.form("my_form"):
   st.write("Input what if values")

   mo_charges = st.text_input('Monthly Charges', 900)
   tot_charges = st.text_input('Total Charges', 5100)

   # Every form must have a submit button.
   submitted = st.form_submit_button("Submit")
   if submitted:
        predict_sql = f"""
        SELECT {udf_prefix}PREDICT_CHURN({mo_charges}, {tot_charges}) as churn_prediction
        """

        prediction_df = snowpark_query(session, predict_sql, non_select=True)
        

        st.dataframe(prediction_df)




