# About


### Structure

### Locally testing Functions


### Locally testing streamlit
from root:
```bash
streamlit run app/src/libraries/streamlit.py

```

### Deployment Steps

Install dependencies
```bash
pip install -r requirements.txt

```

Update Files:
- app/deploy/to_stage.yml
- src/ files
- sf_account.config

```bash
python app/deploy/deploy.py --env dev
python app/deploy/deploy.py --env prod

```


Running app/deploy/deploy.py will also download the model referenced in the UDF to the app/src/libraries/models folder
