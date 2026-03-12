import warnings
# Silence the urllib3 LibreSSL warning before anything else loads
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

from .auth import login
from .data import load_dataset, list_datasets, load_metadata, load_chem, load_experiment, query, describe

__version__ = "0.1.0"
