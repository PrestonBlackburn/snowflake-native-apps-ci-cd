# Snowflake Native App CI/CD

### About
<b>What</b>: CI/CD Snowflake native app deployment example with GitHub Actions  
<b>Why</b>: Currently there are no examples of deploying native apps in a CI/CD process  
<b>How</b>: Automate some of the manual deployment steps with python. Template out key variables to a separte yaml file

<br/>

### Structure
```txt
| -- .github/workflows        <-- github workflow for automation
| -- app
|    | -- deploy               <-- deployment scripts
|    | -- src                
|    |    | -- libraries       <-- core functions + streamlit app
|    |    | -- scripts         <-- setup files executed by snowflake app
|    | -- test                 <-- for future testing files
|
| -- initial_modeling          <-- notebooks to be ran before deployment pipeline
|    | -- initial_load.ipynb   <-- initial load of data to snowflake
|    | -- training.ipynb       <-- create/train model + create model object
|
| -- test_data                  <-- datasets for training and testing

```

<br/>

### Locally testing streamlit
(need to create objects first)
from root:
```bash
streamlit run app/src/libraries/streamlit.py
```
If there is an active session then we know that it is running on Snowflake and not locally, so we can handle running the app on Snowflake vs locally separatly. 

<br/>

### Initial Deployment Steps

1. Install dependencies
```bash
pip install -r requirements.txt
```

2. Update Files:
- `app/deploy/to_stage.yml`  - Key deployment variables to update. Parameters are used in deploy.py file
- `src/ files`               - This is the core where you will build out your own app
- `sf_account.config`        - Your snowflake credentials used for creating the app


3. deploy to environments:
```bash
python app/deploy/deploy.py --env dev
python app/deploy/deploy.py --env prod

```
Running app/deploy/deploy.py will also download the model referenced in the UDF to the app/src/libraries/models folder

<br/>

### CI/CD deployment Steps
1. Convert your `sf_account.config` file to a base64 string so it can be uploaded to GitHub actions secrets
(on linux)
```bash
cat sf_account.config | base64
```
example results: W0RFRkFVTFRdDQphY2NvdW50PWdqemtqb2stdGo1NDkxOA0KdXNlcj1wcmVzdG9uYg0KcGFzc3dv

2. Upload the secrests string as a Github Actions secret. Make sure your secrets name lines up with the github actions deploy.yml file

3. Push changes to the dev or main branch to trigger the automated deployment



