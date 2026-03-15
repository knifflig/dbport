"""Secondary (driven) adapters — concrete implementations of domain ports.

catalog/   : Iceberg REST catalog (ICatalog)
compute/   : DuckDB local compute (ICompute)
lock/      : dbport.lock TOML persistence (ILockStore)
metadata/  : metadata.json materialization + Iceberg attachment (IMetadataStore)
"""
