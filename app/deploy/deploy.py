
# upload package to stage:
from snowflake.snowpark import Session
import configparser
import argparse
import yaml
import os

import logging

logging.basicConfig(filename='deployment.log', level=logging.INFO)


def get_app_config(deploy_yaml_path):
    with open(deploy_yaml_path, "r") as f:
        stage_dict = yaml.safe_load(f)
    # print(stage_dict)

    return stage_dict


def run_bootstrap_sql(session, stage_dict, env) -> None:

    if env == 'dev':
        app_name = stage_dict['app_dev'].upper()

    if env == 'prod':
        app_name = stage_dict['app'].upper()


    stage_dict
    bootstrap_sql = []

    bootstrap_sql.append(f"""CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER('"{app_name}"') COMMENT = '' DISTRIBUTION = 'INTERNAL'; """)
    bootstrap_sql.append(f""" USE DATABASE {app_name}""" )
    bootstrap_sql.append(f""" CREATE OR REPLACE SCHEMA {stage_dict['schema']}""")

    # create app stage
    bootstrap_sql.append(f""" CREATE OR REPLACE STAGE {stage_dict['stage']} 
                                DIRECTORY = ( ENABLE = true ) 
                                COMMENT = '';""")


    for sql in bootstrap_sql:
        session.sql(sql).collect()




def get_package_files(stage_dict, env):

    if env == 'dev':
        app_name = stage_dict['app_dev']

    if env == 'prod':
        app_name = stage_dict['app']

    # or maybe just deploy everything in the src dir
    #source_dir = "app/src"
    source_dir = stage_dict['local_root']
    source_paths = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            source_paths.append(f"{root}/{file}".replace("\\", "/"))

    #print(source_paths)

    sf_paths = []
    for path in source_paths:
        sf_path = path.split(f"{source_dir}/")[-1]
        sf_path = sf_path.split("/")[:-1]
        sf_path = "/".join(sf_path)
        print(sf_path)
        #sf_path_full = f"@{stage_dict['app']}.{stage_dict['schema']}.{stage_dict['stage']}/{stage_dict['version_folder']}/{sf_path}"
        sf_path_full = f"@{app_name}.{stage_dict['schema']}.{stage_dict['stage']}/{sf_path}"
        sf_paths.append(sf_path_full)

    #print(sf_paths)

    return source_paths, sf_paths




def push_files_to_sf(session, source_paths, sf_paths):
    for i, source in enumerate(source_paths):

        put_result = session.file.put(source, sf_paths[i], auto_compress = False, overwrite=True)
        print(put_result[0].status)





def get_staged_models(session, source_stage, local_path):
    
    # ex: get_result1 = session.file.get("@myStage/prefix1/test2CSV.csv", "tests/downloaded/target1")
    session.file.get(source_stage, local_path)
    # put_result = session.file.put(local_path, sf_path, auto_compress = False, overwrite=True )
    # print(put_result[0].status)



def share_content(session, stage_dict, env):

    if env == 'dev':
        app_name = stage_dict['app_dev']

    if env == 'prod':
        app_name = stage_dict['app']

    # create the share
    data_share_sql = f"""create schema if not exists {app_name}.{stage_dict['data_sharing_schema']}  """
    session.sql(data_share_sql).collect()

    # share the data as views
    for source_name in stage_dict['shared_data']:
        table_name = source_name.split(".")[-1]
        shared_view_sql = f""" create or replace view {app_name}.{stage_dict['data_sharing_schema']}.{table_name}
                            as select * from  {source_name} """
        
        session.sql(shared_view_sql).collect()

    
    # grant usage to the share to the app package
    schema_usage_sql = f""" grant usage on schema {app_name}.{stage_dict['data_sharing_schema']} to share in application package {app_name} """
    session.sql(schema_usage_sql).collect()

    for source_name in stage_dict['shared_data']:
        ref_db = source_name.split(".")[0]
        ref_usage_sql = f""" grant reference_usage on database {ref_db} to share in application package {app_name} """
        session.sql(ref_usage_sql).collect()

    for source_name in stage_dict['shared_data']:
        table_name = source_name.split(".")[-1]
        view_privileges_sql = f""" grant select on view {app_name}.{stage_dict['data_sharing_schema']}.{table_name} to share in application package {app_name}"""
        session.sql(view_privileges_sql).collect()




    

        
def run_package(session, stage_dict, env):

    if env == 'dev':
        app_name = stage_dict['app_dev']

    if env == 'prod':
        app_name = stage_dict['app']

    # creates the package
    # create_sql = f""" CREATE OR REPLACE APPLICATION {stage_dict['app']}_APP
    #                     FROM APPLICATION PACKAGE {stage_dict['app']}
    #                     USING '@{stage_dict['app']}.{stage_dict['schema']}.{stage_dict['stage']}/{stage_dict['version_folder']}';"""
    create_sql = f""" CREATE APPLICATION {app_name}_APP
                        FROM APPLICATION PACKAGE {app_name}
                        USING '@{app_name}.{stage_dict['schema']}.{stage_dict['stage']}';"""
    print(create_sql)
    session.sql(create_sql).collect()


    #version_package_sql = f"""ALTER APPLICATION PACKAGE "{stage_dict['app'].upper()}" ADD VERSION "{stage_dict['version_folder']}" USING '@{stage_dict['app']}.{stage_dict['schema']}.{stage_dict['stage']}/{stage_dict['version_folder']}' LABEL = ''"""
    version_package_sql = f"""ALTER APPLICATION PACKAGE "{app_name.upper()}" ADD VERSION "{stage_dict['version_folder']}" USING '@{app_name}.{stage_dict['schema']}.{stage_dict['stage']}' LABEL = ''"""
    
    print(version_package_sql)
    session.sql(version_package_sql).collect()

    release_sql = f"""ALTER APPLICATION PACKAGE "{app_name.upper()}" SET DEFAULT RELEASE DIRECTIVE VERSION = "{stage_dict['version_folder']}" PATCH = 0"""
    print(release_sql)
    session.sql(release_sql).collect()






if __name__ == "__main__":

    # run from root dir:
    deploy_yaml_path = "app/deploy/to_stage.yml"
    conf_path = 'sf_account.config'

    # verify path
    secrets_exist = os.path.isfile(conf_path)
    print("secrets file exists: ", secrets_exist)

    config = configparser.ConfigParser()
    config.read(conf_path)
    session = Session.builder.configs(dict(config['DEFAULT'])).create()  

    stage_dict = get_app_config(deploy_yaml_path)



    parser = argparse.ArgumentParser(description='run the deployment script')
    parser.add_argument('-e', '--env', help='set the env for deployment')

    args = parser.parse_args()
    print(args.env)

    env = args.env

    if env == 'dev':
        app_name = stage_dict['app_dev']

    if env == 'prod':
        app_name = stage_dict['app']


    source_paths, sf_paths = get_package_files(stage_dict, env)

    print(source_paths, sf_paths)

    run_bootstrap_sql(session, stage_dict, env)

    share_content(session, stage_dict, env)

    
    # download model so it will also be pushed
    model_path = "@CUSTOMER_DB.CHURN.MODELS/regressor.pkl"
    local_path = "app/src/libraries/models"
    get_staged_models(session, model_path, local_path)

    push_files_to_sf(session, source_paths, sf_paths)


    run_package(session, stage_dict, env)







