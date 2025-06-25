from importlib import resources
from pathlib import Path
import yaml

# locate and read the yaml file listing selected features
if __package__:
    _yaml_path = resources.files(__package__).joinpath("candidate-columns.yml")
elif __file__:
    _yaml_path = Path(__file__).with_name("candidate-columns.yml")

# get columns
KEEP_COLS = yaml.safe_load(open(_yaml_path))["columns"]

TARGET = "nutrition_grade_fr"
PREDICTORS = [c for c in KEEP_COLS if c != TARGET]
