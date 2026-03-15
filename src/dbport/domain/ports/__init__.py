"""Domain ports — abstract interfaces (typing.Protocol) that secondary adapters must satisfy.

Ports are defined here in the domain; adapters in adapters/secondary/ implement them.
The domain never imports from adapters.
"""

from .catalog import ICatalog
from .compute import ICompute
from .lock import ILockStore
from .metadata import IMetadataStore

__all__ = [
    "ICatalog",
    "ICompute",
    "ILockStore",
    "IMetadataStore",
]
