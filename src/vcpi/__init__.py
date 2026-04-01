# ruff: noqa: E402
import warnings

# Silence the urllib3 LibreSSL warning before anything else loads
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")

from .auth import login as login
from .data import (
    describe as describe,
)
from .data import (
    list_datasets as list_datasets,
)
from .data import (
    load_chem as load_chem,
)
from .data import (
    load_dataset as load_dataset,
)
from .data import (
    load_experiment as load_experiment,
)
from .data import (
    load_metadata as load_metadata,
)
from .data import (
    query as query,
)

__version__ = "0.2.0"
