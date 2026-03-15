"""Adapters layer — concrete implementations of domain ports.

primary/    : driving adapters (DBPort, ColumnRegistry)
secondary/  : driven adapters (Iceberg catalog, DuckDB, dbport.lock, metadata)
"""
