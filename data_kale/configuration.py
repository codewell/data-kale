from pathlib import Path

from functools import lru_cache

import toml


@lru_cache()
def configuration():
    with open(Path.home() / '.kale.toml') as f:
        value = toml.load(f)

        return value