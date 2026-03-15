"""Infrastructure layer — cross-cutting concerns.

No domain logic here. Provides:
- credentials : WarehouseCreds (pydantic-settings, env var resolution)
- logging     : auto-logging setup wired from DBPort.__init__
"""
