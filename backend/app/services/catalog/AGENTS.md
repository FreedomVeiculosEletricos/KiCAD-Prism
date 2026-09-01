# Catalog decomposition

This directory is the target package for the incremental catalog split. It is
an empty package marker in PR 1: the running implementation remains the
compatibility singleton in `backend/app/services/component_catalog_service.py`,
backed by `backend/app/services/component_catalog_service_postgres.py` and
`backend/app/services/component_catalog_domain.py`.

## Future layering

Later slices may add narrow, dependency-injected collaborators in four layers:

- foundation: stable value shaping, hashing, signing, and shared primitives;
- persistence: PostgreSQL reads/writes and transaction boundaries;
- domain: component, revision, asset, import, validation, release, and export
  operations;
- compatibility facade: an explicit adapter for the existing public callable
  surface, composed by the stable root outside this package.

The layer names describe the destination architecture, not files that exist
yet. Keep each new collaborator pointed toward lower layers only. In
particular, nothing in this package may import or depend back on the three
legacy catalog modules, directly or through a relative import. Avoid mixins and
dynamic `__getattr__` delegation; pass dependencies explicitly.

## Navigation

Start from the public API routes in `backend/app/api/catalog_admin.py` and
`backend/app/api/remote_provider.py`, then follow worker dispatch in
`backend/app/services/catalog_worker_tasks.py`. The PostgreSQL behavior and
characterization contracts are covered by
`backend/tests/test_component_catalog_postgres_integration.py`; the private
caller and module-size ratchets are checked by
`backend/tests/test_catalog_architecture.py` and
`scripts/check_catalog_architecture.py`.
