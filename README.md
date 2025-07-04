
# Welcome to your CDK Python project!

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!


------------------------------------------------------------------
### CDK synth command
cdk synth -a "python infrastructure/app.py"
cdk deploy -a "python infrastructure/app.py" --require-approval never

### Partition strategy 

All processed Parquet shards are written under the **`processed/`** prefix using
**two Hive-style partitions**:

s3://<processed-bucket>/processed/
└── year=<YYYY>/
└── country=<slug>/
part-0000.parquet

sql
Copy
Edit

* **`year`** – extracted from `created_t` (UTC epoch → calendar year).  
* **`country`** – first token of `countries_tags`, lower-cased, language
  prefix stripped (`en:united-states` → `united-states`).

This layout keeps partition count reasonable (≈ years × countries) while
matching common query filters (time window + user locale). It also minimises
Athena scan size by enabling **partition pruning**.


### Canonical Feature Subset (Day-1)
columns:
  - energy-kcal_100g
  - fat_100g
  - saturated-fat_100g
  - carbohydrates_100g
  - sugars_100g
  - fiber_100g
  - proteins_100g
  - sodium_100g
  - additives_n
  - fruits-vegetables-nuts_100g
  - ingredients_from_palm_oil_n
  - ingredients_that_may_be_from_palm_oil_n
  - main_category
  - categories_tags
  - labels_tags
  - packaging_tags
  - brands_tags
  - countries_tags
  - serving_size
  - created_t
# target
  - nutrition_grade_fr

> **Day-1 note:** The initial load is designed to run from a developer workstation.  
> Future versions may move to an ephemeral EC2 runner or Step Functions task.

## Validation metrics (Day-1)

After each ingest run:

```bash
python src/ingestion/validate_ingest.py --bucket <PROC_BUCKET> --profile <AWS_PROFILE>

PWS
$Env:PYTHONPATH = "$PWD\src;$Env:PYTHONPATH"
✔ Ingested 3,902,201 rows in 40.6 min (1,601 rows/s)

 python -m ingestion.validate_ingest --bucket nutrisage-processed-352364310453 --profile nutrisage-dev

  python -m ingestion.ingest_nutrisage --input data/openfoodfacts-products.jsonl.gz --raw-bucket nutrisage-raw-352364310453 --proc-bucket nutrisage-processed-352364310453 --profile nutrisage-dev --chunk-rows 50_000

Validation
(.venv) (base) PS C:\Users\Asaeez\Documents\Zohreh\personal_project\nutrisage-mlops> python -m ingestion.validate_ingest --bucket nutrisage-processed-352364310453 --profile nutrisage-dev
✔ Summary validation OK – 3,902,201 rows; cols & types match