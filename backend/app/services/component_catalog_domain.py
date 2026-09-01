from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import shutil
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from app.core.config import settings
from app.services.catalog.component_history import CatalogComponentHistoryReads
from app.services.catalog.component_read_models import (
    CatalogComponentReadModels,
    SUPPLY_KIND_LOCAL,
    SUPPLY_KIND_VENDOR,
    SUPPLY_LOCAL_SOURCE_NAMES,
    SUPPLY_VENDOR_SOURCE_NAMES,
    supply_source_payload as _supply_source_payload,
)
from app.services.catalog.component_queries import CatalogComponentQueries
from app.services.catalog.health import CatalogHealth
from app.services.catalog.klc_validation import (
    VALIDATION_SEVERITY_ERROR,
    VALIDATION_SEVERITY_INFO,
    VALIDATION_SEVERITY_WARNING,
    CatalogKlcValidation,
)
from app.services.catalog.asset_browser import CatalogAssetBrowser
from app.services.catalog.asset_files import (
    CatalogAssetFiles,
    content_type_for_asset as _content_type_for_asset,
    discover_footprint_name_in_text as _discover_footprint_name_in_text,
    discover_symbol_names_in_text as _discover_symbol_names_in_text,
)
from app.services.catalog.asset_imports import CatalogAssetImports
from app.services.catalog.asset_links import CatalogAssetLinks
from app.services.catalog.asset_registry import CatalogAssetRegistry
from app.services.catalog.asset_types import (
    PLACE_REQUIRED_ASSET_TYPES,
    PREVIEW_KIND_FOOTPRINT,
    PREVIEW_KIND_SYMBOL,
    SUPPORTED_ASSET_TYPES,
)
from app.services.catalog.kicad_cli import KicadCliRunner
from app.services.catalog.locking import CatalogLockOperations, NoopCatalogLocks
from app.services.catalog.metadata_batch_application import CatalogMetadataBatchApplication
from app.services.catalog.metadata_batches import CatalogMetadataBatches
from app.services.catalog.metadata_batch_staging import CatalogMetadataBatchStaging
from app.services.catalog.metadata_csv import (
    CSV_ASSET_COLUMNS,
    CSV_REQUIRED_COLUMNS,
    CSV_SPREADSHEET_TEXT_GUARD,
    CatalogMetadataCsv,
)
from app.services.catalog.inventory_csv import CatalogInventoryCsv
from app.services.catalog.metadata_fields import CatalogMetadataFields
from app.services.catalog.metadata_grid import CatalogMetadataGrid
from app.services.catalog.metadata_schema import (
    BUILTIN_METADATA_FIELDS,
    CatalogMetadataSchema,
    METADATA_FIELD_TYPES,
    METADATA_SCHEMA_VERSION,
    SYMBOL_METADATA_LABEL_TO_KEY,
)
from app.services.catalog.metadata_normalization import (
    IDENTITY_KIND_MPN,
    IDENTITY_KIND_PROVISIONAL_IPN,
    MPN_SOURCE_MANUFACTURER,
    MPN_SOURCE_PROVISIONAL_IPN,
    dedupe,
    metadata_keywords,
    metadata_search_document,
    normalize_identity_value,
    normalize_metadata,
)
from app.services.catalog.normalization import (
    json_loads as _json_loads,
    preview_base_kind as _preview_base_kind,
    preview_kind as _preview_kind,
    preview_unit as _preview_unit,
    preview_unit_label as _preview_unit_label,
    sha256_bytes as _sha256_bytes,
    sha256_file as _sha256_file,
    sanitize_name as _sanitize_name,
    slugify as _slugify,
    utc_now_iso as _utc_now_iso,
)
from app.services.catalog.project_import_sessions import CatalogProjectImportSessions
from app.services.catalog.project_import_matching import CatalogProjectImportMatching
from app.services.catalog.project_import_assets import CatalogProjectImportAssets
from app.services.catalog.project_import_acceptance import CatalogProjectImportAcceptance
from app.services.catalog.preview_pipeline import (
    CatalogPreview,
    CatalogPreviewPipeline,
    PREVIEW_PIPELINE_VERSION,
)
from app.services.catalog.preview_renderer import (
    CatalogPreviewRenderer,
    PREVIEW_STATUS_FAILED,
    PREVIEW_STATUS_READY,
)
from app.services.catalog.preview_store import CatalogPreviewStore
from app.services.catalog.release_workflow import CatalogReleaseWorkflow
from app.services.catalog.representations import CatalogRepresentations
from app.services.catalog.revision_comparison import CatalogRevisionComparison
from app.services.catalog.revision_finalization import CatalogRevisionFinalizer
from app.services.catalog.revision_kernel import (
    CatalogRevisionKernel,
    LEGACY_WORKFLOW_STAGE_MAP,
    REVISION_MANIFEST_A0,
    REVISION_MANIFEST_A1,
    REVISION_MANIFEST_A2,
    REVISION_MANIFEST_A3,
    WORKFLOW_STAGES,
    normalize_workflow_stage,
)
from app.services.catalog.runtime import (
    CatalogRuntime, DBL_EXPORT_DIRNAME, DEFAULT_STORE_DIRNAME, KLC_VALIDATION_DIRNAME,
    _ASSET_BROWSE_CACHE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)

SOURCE_MANUAL = "manual"
SOURCE_EXTERNAL = "external"
RELEASE_STATES = WORKFLOW_STAGES

STATE_METADATA_ONLY = "metadata_only"
STATE_FILES_PARTIAL = "files_partial"
STATE_PLACE_READY = "place_ready"

VALIDATION_STATUS_PASSED = "passed"
VALIDATION_STATUS_WARNING = "warning"
VALIDATION_STATUS_FAILED = "failed"
VALIDATION_STATUS_SKIPPED = "skipped"
VALIDATION_STATUS_NOT_RUN = "not_run"
KLC_RELEASE_GATE_VALUES = {"off", "warn", "block"}

SYMBOL_METADATA_FIELD_ORDER: tuple[str, ...] = (
    "Value",
    "Description",
    "Datasheet",
    "Manufacturer",
    "Manufacturer Part Number",
    "Vendor",
    "Vendor Part Number",
    "Mass (g)",
    "RQjC (C/W)",
    "RQjC_top (C/W)",
    "Temp_max (C)",
    "Temp_min (C)",
    "Power Dissipation (W)",
    "Rate",
    "SAP Code",
)

DBL_COMMON_COLUMNS: tuple[str, ...] = (
    "Part Number",
    "Part Number Nocolon",
    "Comment",
    "Value",
    "Manufacturer",
    "Manufacturer Part Number",
    "PackageDescription",
    "Status",
    "Part Description",
    "Datasheet",
    "LibSymbol",
    "LibFootprint",
)

_TOP_LEVEL_PROPERTY_RE = re.compile(r'^([ \t]+)\(property "([^"]+)" ')


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _remote_library_nickname(library_name: str) -> str:
    prefix = _sanitize_name(settings.REMOTE_PROVIDER_LIBRARY_PREFIX, "remote").lower()
    library = _sanitize_name(library_name, "library").lower()
    return f"{prefix}_{library}"


def _escape_symbol_property_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _symbol_property_block(name: str, value: str, *, indent: str = "    ", hidden: bool = True) -> str:
    hide = " hide" if hidden else ""
    child_indent = f"{indent}  "
    return (
        f'{indent}(property "{name}" "{_escape_symbol_property_value(value)}" (at 0 0 0)\n'
        f'{child_indent}(effects (font (size 1.27 1.27)){hide})\n'
        f"{indent})\n"
    )


def _symbol_metadata_fields(component: dict[str, Any] | None) -> dict[str, str]:
    if not component:
        return {label: "" for label in SYMBOL_METADATA_FIELD_ORDER}
    fields = {label: str(component.get(key) or "") for label, key in SYMBOL_METADATA_LABEL_TO_KEY.items()}
    for key, value in sorted(dict(component.get("extra_fields") or {}).items()):
        normalized_key = str(key).strip()
        if normalized_key and normalized_key not in fields and normalized_key not in {"Reference", "Footprint"}:
            fields[normalized_key] = str(value or "")
    return fields


def _extract_top_level_symbol_properties(header: str) -> tuple[str, list[tuple[str, str]], str, str]:
    lines = header.splitlines(keepends=True)
    prefix_parts: list[str] = []
    property_blocks: list[tuple[str, str]] = []
    trailing = ""
    first_indent = ""
    index = 0

    while index < len(lines):
        line = lines[index]
        match = _TOP_LEVEL_PROPERTY_RE.match(line)
        if not match:
            if property_blocks:
                trailing = "".join(lines[index:])
                break
            prefix_parts.append(line)
            index += 1
            continue

        indent = match.group(1)
        if not first_indent:
            first_indent = indent
        name = match.group(2)
        depth = line.count("(") - line.count(")")
        block_lines = [line]
        index += 1

        while depth > 0 and index < len(lines):
            block_line = lines[index]
            block_lines.append(block_line)
            depth += block_line.count("(") - block_line.count(")")
            index += 1

        property_blocks.append((name, "".join(block_lines)))

    return "".join(prefix_parts), property_blocks, trailing, first_indent or "    "


def _rewrite_symbol_payload(payload: bytes, footprint_ref: str | None, component: dict[str, Any] | None = None) -> bytes:
    text = payload.decode("utf-8")
    first_symbol_index = text.find('(symbol "')
    marker_index = text.find('(symbol "', first_symbol_index + 1) if first_symbol_index != -1 else -1
    if marker_index <= 0:
        header = text
        suffix = ""
    else:
        header = text[:marker_index]
        suffix = text[marker_index:]

    prefix, extracted_blocks, trailing, indent = _extract_top_level_symbol_properties(header)
    if not extracted_blocks:
        return payload

    existing_blocks = {name: block for name, block in extracted_blocks}
    ordered_names = [name for name, _ in extracted_blocks]
    metadata_fields = _symbol_metadata_fields(component)
    custom_blocks = {
        label: _symbol_property_block(label, value, indent=indent, hidden=label != "Value")
        for label, value in metadata_fields.items()
    }
    if footprint_ref:
        custom_blocks["Footprint"] = _symbol_property_block("Footprint", footprint_ref, indent=indent)
    elif "Footprint" in existing_blocks:
        custom_blocks["Footprint"] = existing_blocks["Footprint"]

    for property_name in SYMBOL_METADATA_FIELD_ORDER:
        if property_name not in ordered_names:
            ordered_names.append(property_name)
    for property_name in sorted(set(metadata_fields) - set(SYMBOL_METADATA_FIELD_ORDER)):
        if property_name not in ordered_names:
            ordered_names.append(property_name)
    if "Footprint" not in ordered_names:
        ordered_names.append("Footprint")

    rebuilt_blocks = [
        custom_blocks.get(property_name, existing_blocks.get(property_name, ""))
        for property_name in ordered_names
    ]
    return (prefix + "".join(rebuilt_blocks) + trailing + suffix).encode("utf-8")


def _rewrite_footprint_payload(
    payload: bytes,
    asset: dict[str, Any],
    model_assets: list[dict[str, Any]] | None = None,
) -> bytes:
    text = payload.decode("utf-8")
    models = list(model_assets or [])
    if not models or "(model " not in text:
        return payload
    prefix = _sanitize_name(settings.REMOTE_PROVIDER_LIBRARY_PREFIX, "remote").lower()
    destination = settings.REMOTE_PROVIDER_DESTINATION_DIR.rstrip("/")
    if destination in {"/RemoteLibrary", "$/RemoteLibrary"}:
        destination = "${KIPRJMOD}/RemoteLibrary"
    model_index = 0

    def replace_model(match: re.Match[str]) -> str:
        nonlocal model_index
        if model_index >= len(models):
            return match.group(0)
        model = models[model_index]
        model_index += 1
        model_name = Path(str(model.get("canonical_path") or model.get("name") or "model.step")).name
        model_path = f"{destination}/{prefix}_3d/{model_name}"
        return f'(model "{model_path}"'

    text = re.sub(r'\(model\s+"[^"]+"', replace_model, text)
    return text.encode("utf-8")


def _release_allows_remote(release_status: str) -> bool:
    return release_status == "released"


def _normalize_workflow_stage(stage: str) -> str:
    return normalize_workflow_stage(stage)


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sexpr_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _part_number_nocolon(value: str) -> str:
    cleaned = re.sub(r":+", "_", value.strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned or "PART"


def _dbl_symbol_library_name(part_number: str, symbol_asset: dict[str, Any] | None) -> str:
    if not symbol_asset:
        return ""
    raw = f"Prism_{part_number}_{symbol_asset['target_library']}_{symbol_asset['target_name']}"
    return _sanitize_name(raw, "Prism_Symbol")


class ComponentCatalogDomainService:
    _catalog_locks: CatalogLockOperations = NoopCatalogLocks()
    _revision_kernel: CatalogRevisionKernel = CatalogRevisionKernel(_catalog_locks)
    _revision_comparison: CatalogRevisionComparison = CatalogRevisionComparison(_revision_kernel)
    _component_history_reads: CatalogComponentHistoryReads = CatalogComponentHistoryReads(_revision_kernel)
    _component_read_models: CatalogComponentReadModels = CatalogComponentReadModels(_revision_kernel)
    _component_queries: CatalogComponentQueries = CatalogComponentQueries(_component_read_models)
    _asset_browser: CatalogAssetBrowser = CatalogAssetBrowser()
    _asset_files: CatalogAssetFiles = CatalogAssetFiles()
    _asset_registry: CatalogAssetRegistry = CatalogAssetRegistry()
    _preview_renderer: CatalogPreviewRenderer = CatalogPreviewRenderer()
    _preview_store: CatalogPreviewStore = CatalogPreviewStore()
    _preview_pipeline: CatalogPreviewPipeline = CatalogPreviewPipeline(
        _catalog_locks, _revision_kernel, _component_read_models, _preview_renderer, _preview_store
    )
    _revision_finalizer: CatalogRevisionFinalizer = CatalogRevisionFinalizer(
        _revision_kernel, _preview_pipeline
    )
    _asset_links: CatalogAssetLinks = CatalogAssetLinks(
        _revision_kernel, _preview_pipeline, _revision_finalizer
    )
    _asset_imports: CatalogAssetImports = CatalogAssetImports(
        _revision_kernel, _asset_links, _revision_finalizer, _asset_files, _asset_registry
    )
    _representations: CatalogRepresentations = CatalogRepresentations(
        _revision_kernel, _revision_finalizer
    )
    _klc_validation: CatalogKlcValidation = CatalogKlcValidation(_revision_kernel, _component_read_models)
    _release_workflow: CatalogReleaseWorkflow = CatalogReleaseWorkflow(
        _catalog_locks, _revision_kernel, _component_read_models, _revision_finalizer, _klc_validation
    )
    _catalog_health: CatalogHealth = CatalogHealth(_component_queries, _klc_validation)
    _project_import_sessions: CatalogProjectImportSessions = CatalogProjectImportSessions()
    _project_import_matching: CatalogProjectImportMatching = CatalogProjectImportMatching()
    _project_import_assets: CatalogProjectImportAssets = CatalogProjectImportAssets(_revision_kernel)
    _project_import_acceptance: CatalogProjectImportAcceptance = CatalogProjectImportAcceptance()
    _metadata_schema: CatalogMetadataSchema = CatalogMetadataSchema()
    _metadata_fields: CatalogMetadataFields = CatalogMetadataFields(_metadata_schema)
    _metadata_grid: CatalogMetadataGrid = CatalogMetadataGrid()
    _metadata_csv: CatalogMetadataCsv = CatalogMetadataCsv()
    _inventory_csv: CatalogInventoryCsv = CatalogInventoryCsv()
    _metadata_batches: CatalogMetadataBatches = CatalogMetadataBatches()
    _metadata_batch_staging: CatalogMetadataBatchStaging = CatalogMetadataBatchStaging()
    _metadata_batch_application: CatalogMetadataBatchApplication = CatalogMetadataBatchApplication()

    def __init__(self, store_root: Path | None = None, database_url: str | None = None) -> None:
        self._catalog_runtime = CatalogRuntime(
            store_root=store_root,
            database_path=self._database_path(database_url),
        )
        self._catalog_locks: CatalogLockOperations = NoopCatalogLocks()
        self._revision_kernel = CatalogRevisionKernel(self._catalog_locks)
        self._revision_comparison = CatalogRevisionComparison(self._revision_kernel)
        self._component_history_reads = CatalogComponentHistoryReads(self._revision_kernel)
        self._component_read_models = CatalogComponentReadModels(self._revision_kernel)
        self._component_queries = CatalogComponentQueries(self._component_read_models)
        self._asset_browser = CatalogAssetBrowser()
        self._asset_files = CatalogAssetFiles()
        self._asset_registry = CatalogAssetRegistry()
        self._preview_renderer = CatalogPreviewRenderer()
        self._preview_store = CatalogPreviewStore()
        self._compose_revision_writers()
        self._project_import_sessions = CatalogProjectImportSessions()
        self._project_import_matching = CatalogProjectImportMatching()
        self._project_import_assets = CatalogProjectImportAssets(self._revision_kernel)
        self._project_import_acceptance = CatalogProjectImportAcceptance()
        self._metadata_schema: CatalogMetadataSchema = CatalogMetadataSchema()
        self._metadata_fields: CatalogMetadataFields = CatalogMetadataFields(self._metadata_schema)
        self._metadata_grid: CatalogMetadataGrid = CatalogMetadataGrid()
        self._metadata_csv: CatalogMetadataCsv = CatalogMetadataCsv()
        self._inventory_csv: CatalogInventoryCsv = CatalogInventoryCsv()
        self._metadata_batches = CatalogMetadataBatches()
        self._metadata_batch_staging = CatalogMetadataBatchStaging()
        self._metadata_batch_application = CatalogMetadataBatchApplication()

    def _compose_revision_writers(self) -> None:
        """Wire the collaborators that write revisions on top of this instance's kernel.

        The PostgreSQL subclass swaps in transactional locks and a fresh kernel
        after ``super().__init__``; it calls this again so every writer shares
        that kernel instead of the no-op base wiring.
        """
        self._preview_pipeline = CatalogPreviewPipeline(
            self._catalog_locks,
            self._revision_kernel,
            self._component_read_models,
            self._preview_renderer,
            self._preview_store,
        )
        self._revision_finalizer = CatalogRevisionFinalizer(self._revision_kernel, self._preview_pipeline)
        self._asset_links = CatalogAssetLinks(
            self._revision_kernel, self._preview_pipeline, self._revision_finalizer
        )
        self._asset_imports = CatalogAssetImports(
            self._revision_kernel,
            self._asset_links,
            self._revision_finalizer,
            self._asset_files,
            self._asset_registry,
        )
        self._representations = CatalogRepresentations(self._revision_kernel, self._revision_finalizer)
        self._klc_validation = CatalogKlcValidation(self._revision_kernel, self._component_read_models)
        self._release_workflow = CatalogReleaseWorkflow(
            self._catalog_locks,
            self._revision_kernel,
            self._component_read_models,
            self._revision_finalizer,
            self._klc_validation,
        )
        self._catalog_health = CatalogHealth(self._component_queries, self._klc_validation)

    def _runtime_for_compat(self) -> CatalogRuntime:
        """Lazily support legacy ``__new__``-constructed test doubles."""
        runtime = self.__dict__.get("_catalog_runtime")
        if runtime is None:
            runtime = CatalogRuntime()
            self.__dict__["_catalog_runtime"] = runtime
        return runtime

    @property
    def _store_root(self) -> Path:
        return self._runtime_for_compat().store_root

    @_store_root.setter
    def _store_root(self, value: Path) -> None:
        self._runtime_for_compat().store_root = Path(value)

    @property
    def _browse_cache(self) -> dict[str, tuple[float, list[str]]]:
        return self._runtime_for_compat().browse_cache

    @_browse_cache.setter
    def _browse_cache(self, value: dict[str, tuple[float, list[str]]]) -> None:
        self._runtime_for_compat().browse_cache = value

    def _database_path(self, database_url: str | None) -> Path:
        _ = database_url
        return Path("/dev/null")

    @property
    def store_root(self) -> Path:
        return self._store_root

    @property
    def db_path(self) -> Path:
        return self._runtime_for_compat().db_path

    @property
    def export_root(self) -> Path:
        return self._runtime_for_compat().export_root

    @property
    def validation_root(self) -> Path:
        return self._runtime_for_compat().validation_root

    def initialize(self) -> None:
        raise NotImplementedError("Use ComponentCatalogPostgresService")

    def close(self) -> None:
        self._runtime_for_compat().close()

    def _ensure_storage_dirs(self) -> None:
        self._runtime_for_compat().ensure_storage_dirs()

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        raise NotImplementedError("Catalog persistence must provide a PostgreSQL connection")
        yield  # pragma: no cover

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """Supported connection scope for maintenance scripts and repository callers.

        The caller owns the transaction: commit or roll back before leaving the
        block.  Package collaborators accept this connection alongside
        ``runtime`` so scripts do not need the service's private helpers.
        """
        with self._connect() as conn:
            yield conn

    @property
    def runtime(self) -> CatalogRuntime:
        """Paths and process-local caches shared with the catalog package."""
        return self._runtime_for_compat()

    @property
    def revisions(self) -> CatalogRevisionKernel:
        return self._revision_kernel

    @property
    def asset_files(self) -> CatalogAssetFiles:
        return self._asset_files

    @property
    def asset_registry(self) -> CatalogAssetRegistry:
        return self._asset_registry

    @property
    def previews(self) -> CatalogPreviewPipeline:
        return self._preview_pipeline

    def _resolve_kicad_cli(self) -> str | None:
        return KicadCliRunner.resolve(self._runtime_for_compat())

    def _run_kicad_cli(self, args: list[str]) -> tuple[bool, str]:
        return KicadCliRunner.run(self._runtime_for_compat(), args)

    def _preview_output_path(self, asset_id: str, kind: str) -> Path:
        return self._preview_pipeline.preview_output_path(self._runtime_for_compat(), asset_id, kind)

    def _preview_version_path(self, asset_id: str, kind: str, sha256: str) -> Path:
        return self._preview_pipeline.preview_version_path(
            self._runtime_for_compat(), asset_id, kind, sha256
        )

    def _preview_generator_identity(self, kind: str) -> dict[str, str]:
        return self._preview_pipeline.generator_identity(self._runtime_for_compat(), kind)

    def _asset_root(self, asset_type: str) -> Path:
        return self._asset_files.asset_root(self._runtime_for_compat(), asset_type)

    def _unique_slug(self, conn: Any, base: str) -> str:
        self._catalog_locks.lock_slug_allocation(conn, base)
        slug = _slugify(base or "component")
        candidate = slug
        counter = 2
        while conn.execute("SELECT 1 FROM components WHERE slug = %s", (candidate,)).fetchone():
            candidate = f"{slug}-{counter}"
            counter += 1
        return candidate

    def _lock_component_identity(self, conn: Any, manufacturer: str, mpn: str) -> None:
        self._catalog_locks.lock_component_identity(conn, manufacturer, mpn)

    def _lock_component_for_mutation(self, conn: Any, component_id: str) -> None:
        self._catalog_locks.lock_component_for_mutation(conn, component_id)

    def _assert_component_identity_available(
        self,
        conn: Any,
        *,
        manufacturer: str,
        mpn: str,
        name: str = "",
        identity_kind: str = IDENTITY_KIND_MPN,
        identity_source: str = "",
        source_internal_part_number: str = "",
        component_id: str = "",
        acquire_identity_lock: bool = True,
    ) -> None:
        """Reject a second component with the same orderable or provisional identity."""
        _ = name
        normalized_manufacturer = normalize_identity_value(manufacturer)
        normalized_part_number = normalize_identity_value(
            mpn if identity_kind == IDENTITY_KIND_MPN else source_internal_part_number
        )
        if acquire_identity_lock:
            self._lock_component_identity(
                conn,
                normalized_manufacturer if identity_kind == IDENTITY_KIND_MPN else identity_source,
                normalized_part_number,
            )
        existing = conn.execute(
            """
            SELECT id
            FROM components
            WHERE identity_kind = %s
              AND normalized_manufacturer = %s
              AND normalized_part_number = %s
              AND identity_source = %s
              AND id <> %s
            LIMIT 1
            """,
            (
                identity_kind,
                normalized_manufacturer if identity_kind == IDENTITY_KIND_MPN else "",
                normalized_part_number,
                identity_source if identity_kind == IDENTITY_KIND_PROVISIONAL_IPN else "",
                component_id,
            ),
        ).fetchone()
        if existing:
            raise ValueError("A component with this manufacturer-part identity already exists")

    def _component_row(self, conn: Any, component_id: str) -> dict[str, Any] | None:
        return self._revision_kernel.component_row(conn, component_id)

    def _revision_row(self, conn: Any, revision_id: str) -> dict[str, Any] | None:
        return self._revision_kernel.revision_row(conn, revision_id)

    def _active_revision_row(
        self,
        conn: Any,
        component_id: str,
        *,
        released: bool = False,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        return self._revision_kernel.active_revision_row(conn, component_id, released=released)

    def _append_audit_event(
        self,
        conn: Any,
        *,
        component_id: str,
        revision_id: str,
        event_type: str,
        actor: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        return self._revision_kernel.append_audit_event(
            conn,
            component_id=component_id,
            revision_id=revision_id,
            event_type=event_type,
            actor=actor,
            details=details,
        )

    def _revision_manifest_hash(self, conn: Any, revision_id: str) -> str:
        return self._revision_kernel.revision_manifest_hash(conn, revision_id)

    def _finalize_revision(
        self,
        conn: Any,
        *,
        component_id: str,
        revision_id: str,
        event_type: str,
        actor: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        self._revision_finalizer.finalize_revision(
            conn,
            self._runtime_for_compat(),
            component_id=component_id,
            revision_id=revision_id,
            event_type=event_type,
            actor=actor,
            details=details,
        )

    def _clone_revision(
        self,
        conn: Any,
        component_id: str,
        *,
        actor: str = "",
        change_kind: str = "edit",
        change_summary: str = "",
        expected_revision_id: str = "",
    ) -> dict[str, Any]:
        return self._revision_kernel.clone_revision(
            conn,
            component_id,
            actor=actor,
            change_kind=change_kind,
            change_summary=change_summary,
            expected_revision_id=expected_revision_id,
        )

    def _load_assets_for_revision(self, conn: Any, revision_id: str) -> list[dict[str, Any]]:
        return self._revision_kernel.load_assets_for_revision(conn, revision_id)

    def _load_representations_for_revision(
        self,
        conn: Any,
        revision_id: str,
        *,
        assets: list[dict[str, Any]] | None = None,
        previews: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        return self._component_read_models.load_representations_for_revision(
            conn, revision_id, assets=assets, previews=previews
        )

    def _local_inventory(self, conn: Any, component_id: str) -> dict[str, Any] | None:
        return self._component_read_models.local_inventory(conn, component_id)

    def _supply_sources(self, conn: Any, component_id: str) -> list[dict[str, Any]]:
        return self._component_read_models.supply_sources(conn, component_id)

    def _load_previews_for_assets(self, conn: Any, asset_ids: list[str]) -> list[dict[str, Any]]:
        return self._component_read_models.load_previews_for_assets(conn, asset_ids)

    def _load_previews_for_revision(self, conn: Any, revision_id: str) -> list[dict[str, Any]]:
        return self._component_read_models.load_previews_for_revision(conn, revision_id)

    def _load_preview_evidence_for_revision(self, conn: Any, revision_id: str) -> list[dict[str, Any]]:
        return self._revision_kernel.load_preview_evidence_for_revision(conn, revision_id)

    def _latest_validation_runs_for_assets(
        self,
        conn: Any,
        revision_id: str,
        asset_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        return self._component_read_models.latest_validation_runs_for_assets(
            conn, revision_id, asset_ids
        )

    def _validation_run_payload(self, row: dict[str, Any], *, include_findings: bool = False, conn: Any | None = None) -> dict[str, Any]:
        return self._component_read_models.validation_run_payload(
            row, include_findings=include_findings, conn=conn
        )

    def _validation_findings_payload(self, conn: Any, run_id: str) -> list[dict[str, Any]]:
        return self._component_read_models.validation_findings_payload(conn, run_id)

    def _component_validation_summary(
        self,
        conn: Any,
        revision_id: str,
        assets: list[dict[str, Any]],
        *,
        preloaded_runs: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return self._component_read_models.component_validation_summary(
            conn,
            revision_id,
            assets,
            preloaded_runs=preloaded_runs,
        )

    def _availability(self, assets: list[dict[str, Any]], release_status: str, is_active: bool) -> tuple[str, list[str], bool]:
        return self._component_read_models.availability(assets, release_status, is_active)

    def _component_payload(
        self,
        conn: Any,
        component_row: dict[str, Any],
        revision_row: dict[str, Any],
        *,
        released_view: bool = False,
        preloaded_assets: list[dict[str, Any]] | None = None,
        preloaded_previews: list[dict[str, Any]] | None = None,
        preloaded_validation_runs: dict[str, dict[str, Any]] | None = None,
        representation_id: str = "",
    ) -> dict[str, Any]:
        return self._component_read_models.component_payload(
            conn,
            component_row,
            revision_row,
            released_view=released_view,
            preloaded_assets=preloaded_assets,
            preloaded_previews=preloaded_previews,
            preloaded_validation_runs=preloaded_validation_runs,
            representation_id=representation_id,
        )

    def list_component_revisions(self, component_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.list_component_revisions(conn, component_id)

    def list_component_audit_events(self, component_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.list_component_audit_events(conn, component_id)

    def verify_component_audit_chain(self, component_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.verify_component_audit_chain(conn, component_id)

    def get_component_revision(self, component_id: str, revision_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._component_read_models.get_component_revision(
                conn, component_id, revision_id
            )

    def compare_component_revisions(self, component_id: str, before_revision_id: str, after_revision_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._revision_comparison.compare_component_revisions(
                conn,
                component_id,
                before_revision_id,
                after_revision_id,
            )

    def list_component_usage(self, component_id: str, *, include_history: bool = False) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.list_component_usage(
                conn, component_id, include_history=include_history
            )

    def list_component_review_decisions(self, component_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.list_component_review_decisions(conn, component_id)

    def list_component_release_records(self, component_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._component_history_reads.list_component_release_records(conn, component_id)

    def catalog_preview_path(self, preview_id: str) -> tuple[Path, str] | None:
        self.initialize()
        with self._connect() as conn:
            return self._preview_pipeline.preview_path(conn, self._runtime_for_compat(), preview_id)

    def catalog_asset_source(self, asset_id: str) -> tuple[Path, str, str] | None:
        """One asset's stored bytes, as written -- not the placement rewrite.

        ``get_asset_by_id`` returns what KiCad places: a symbol carries an
        injected footprint reference and the component's fields, a footprint
        carries rewritten 3D-model paths. Those rewrites exist for placement.
        A renderer wants the file as the library holds it, so this resolves the
        canonical path instead of materialising a payload.
        """
        self.initialize()
        with self._connect() as conn:
            return self._asset_imports.asset_source(conn, self._runtime_for_compat(), asset_id)

    def create_project_import_session(
        self,
        *,
        scope: str,
        project_id: str = "",
        project_ids: list[str] | None = None,
        project_revisions: dict[str, str] | None = None,
        source_revision: str = "",
        selection: dict[str, Any] | None = None,
        actor: str = "",
    ) -> dict[str, Any]:
        if scope not in {"component", "project", "all-projects", "folder"}:
            raise ValueError("Unsupported project import scope")
        if scope in {"component", "project"} and not project_id:
            raise ValueError("project_id is required for this import scope")
        if scope == "component" and not selection:
            raise ValueError("selection is required for component import")
        self.initialize()
        now = _utc_now_iso()
        session_id = str(uuid.uuid4())
        with self._connect() as conn:
            self._project_import_sessions.create_session(
                conn,
                session_id=session_id,
                scope=scope,
                project_id=project_id,
                project_ids=project_ids or ([project_id] if project_id else []),
                project_revisions=project_revisions or {},
                source_revision=source_revision,
                selection=selection or {},
                actor=actor,
                now=now,
            )
            conn.commit()
        return self.get_project_import_session(session_id) or {}

    def get_project_import_session(self, session_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._project_import_sessions.get_session(conn, session_id)

    def list_project_import_sessions(self, *, created_by: str = "", include_all: bool = False) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            session_ids = self._project_import_sessions.list_session_ids(
                conn,
                created_by=created_by,
                include_all=include_all,
            )
        return [session for session_id in session_ids if (session := self.get_project_import_session(session_id)) is not None]

    def update_project_import_session(self, session_id: str, *, status: str, error_message: str = "") -> None:
        if status not in {"queued", "uploading", "scanning", "staged", "failed"}:
            raise ValueError("Unsupported project import session status")
        self.initialize()
        with self._connect() as conn:
            self._project_import_sessions.update_session(
                conn,
                session_id,
                status=status,
                error_message=error_message,
                now=_utc_now_iso(),
            )
            conn.commit()

    def stage_project_import_proposals(self, session_id: str, proposals: list[dict[str, Any]]) -> None:
        self.initialize()
        now = _utc_now_iso()
        with self._connect() as conn:
            self._project_import_sessions.stage_proposals(conn, session_id, proposals, now=now)
            conn.commit()

    def list_project_import_proposals(self, session_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._project_import_sessions.list_proposals(conn, session_id)

    def get_project_import_proposal(self, proposal_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._project_import_sessions.get_proposal(conn, proposal_id)

    def save_project_import_drafts(
        self, session_id: str, drafts: dict[str, dict[str, Any]]
    ) -> int:
        self.initialize()
        if not drafts:
            return 0
        now = _utc_now_iso()
        with self._connect() as conn:
            updated = self._project_import_sessions.save_drafts(
                conn,
                session_id,
                drafts,
                now=now,
            )
            conn.commit()
        return updated

    def search_assets(
        self,
        *,
        asset_type: str,
        query: str = "",
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Find existing catalog assets so an import can reuse one instead of copying it.

        `revision_assets` is a join table onto content-addressed `assets`, so linking
        an existing row is a genuine reference: one 0603 footprint is shared by every
        component that uses it rather than duplicated per import.

        Ordering by usage means the counts have to exist before LIMIT can apply, which
        costs an aggregate over every matching asset. That is affordable once a search
        term has narrowed the set, but not for the empty query the picker fires the
        moment it opens: on a store of ~17k assets that aggregate reads the whole
        revision_assets join and took ~500ms. The unfiltered listing therefore orders
        by the columns idx_assets_kind already covers and counts usage only for the
        page it returns.
        """
        self.initialize()
        plan = self._project_import_assets.prepare_search_assets(
            asset_type=asset_type,
            query=query,
            limit=limit,
        )
        with self._connect() as conn:
            return self._project_import_assets.execute_search_assets(conn, plan)

    def index_project_component_usage(self, proposals: list[dict[str, Any]]) -> dict[str, int]:
        """Index where-used observations even when no import proposal is accepted."""
        self.initialize()
        now = _utc_now_iso()
        with self._connect() as conn:
            result = self._project_import_matching.index_project_component_usage(
                conn,
                proposals,
                observed_at=now,
            )
            conn.commit()
        return result

    def match_component_identities(self, identities: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
        """Resolve manufacturer/MPN identities in one catalog query for import preflight."""
        requested = self._project_import_matching.normalize_identity_requests(identities)
        if not requested:
            return {}
        self.initialize()
        with self._connect() as conn:
            return self._project_import_matching.match_component_identities(conn, requested)

    def accept_project_import_proposal(
        self,
        proposal_id: str,
        *,
        metadata_overrides: dict[str, Any] | None = None,
        asset_selections: dict[str, list[str]] | None = None,
        asset_links: dict[str, str] | None = None,
        actor: str = "",
        change_summary: str = "Import component from project",
    ) -> dict[str, Any]:
        self.initialize()
        proposal = self.get_project_import_proposal(proposal_id)
        if not proposal:
            raise ValueError("Import proposal not found")
        if proposal["status"] != "candidate":
            raise ValueError("Project import proposal has already been resolved")
        normalized_input = self._project_import_acceptance.build_normalized_input(
            proposal,
            metadata_overrides,
        )
        metadata = normalize_metadata(normalized_input)
        candidates_by_type = self._project_import_assets.group_import_assets(proposal)
        # An asset type may instead be satisfied by an existing catalog asset. That is
        # a reference, not a copy: the same assets row is linked into this revision, so
        # one shared 0603 footprint serves every part that uses it.
        requested_links = self._project_import_assets.normalize_import_asset_links(asset_links or {})
        linked_assets: dict[str, dict[str, Any]] = {}
        if requested_links:
            with self._connect() as conn:
                linked_assets = self._project_import_assets.resolve_import_asset_links(conn, requested_links)
        by_type = self._project_import_assets.select_import_assets(
            candidates_by_type,
            asset_selections=asset_selections,
            linked_assets=linked_assets,
            findings=proposal["findings"],
        )
        self._project_import_assets.validate_project_import_asset_paths(
            self._store_root,
            proposal,
            by_type,
        )

        now = _utc_now_iso()
        with self._connect() as conn:
            self._project_import_acceptance.claim_proposal(
                conn,
                proposal_id,
                now=now,
            )
            self._lock_component_identity(conn, metadata["manufacturer"], metadata["mpn"])
            existing = self._project_import_acceptance.find_existing_component(
                conn,
                metadata["manufacturer"],
                metadata["mpn"],
            )
            component_id = str(existing["id"]) if existing else str(uuid.uuid4())
            import_payload = self._project_import_acceptance.build_import_payload(proposal)
            provenance = import_payload["provenance"]
            import_source = import_payload["import_source"]
            external_id = import_payload["external_id"]
            current_revision = (
                self._revision_row(conn, str(self._component_row(conn, component_id)["current_revision_id"]))
                if existing
                else None
            )
            no_content_change = bool(
                current_revision
                and self._project_import_assets.revision_matches_import(
                    conn,
                    current_revision,
                    metadata,
                    by_type,
                )
            )
            if no_content_change and current_revision:
                revision_id = str(current_revision["id"])
            else:
                _, revision_id = self._upsert_component_metadata_row(
                    conn,
                    component_id=component_id,
                    metadata=metadata,
                    now=now,
                    existing_component_id=component_id if existing else None,
                    actor=actor,
                    change_summary=change_summary,
                    finalize_revision=False,
                    source=SOURCE_EXTERNAL,
                    external_source=import_source,
                    external_id=external_id,
                    change_kind="folder_import" if import_source == "folder_snapshot" else "project_import",
                )
                for asset_type, candidates in by_type.items():
                    for asset in candidates:
                        staged_path = Path(str(asset.get("staged_path") or "")).resolve()
                        # Recheck immediately before reading to close the window between
                        # proposal validation and canonical storage.
                        if _sha256_file(staged_path) != str(asset.get("sha256") or ""):
                            raise ValueError(f"Staged {asset_type} asset has changed")
                        payload = staged_path.read_bytes()
                        target_library = str(asset.get("target_library") or "Prism_Imported")
                        target_name = str(asset.get("target_name") or staged_path.stem)
                        if asset_type == "symbol":
                            destination = self._symbol_destination(target_library, target_name)
                        elif asset_type == "footprint":
                            destination = self._footprint_destination(target_library, target_name)
                        elif asset_type in {"3dmodel", "spice"}:
                            destination = self._aux_destination(asset_type, target_library, staged_path.name)
                        else:
                            continue
                        canonical_path = self._write_canonical_file(destination, payload)
                        registered = self._register_asset(
                            conn,
                            asset_type=asset_type,
                            canonical_path=canonical_path,
                            target_library=target_library,
                            target_name=target_name,
                            source_group=f"{import_source}:{proposal['session_id']}",
                        )
                        self._link_asset_to_revision(
                            conn,
                            revision_id,
                            registered,
                            required=asset_type in PLACE_REQUIRED_ASSET_TYPES,
                        )
                for asset_type, existing_asset in linked_assets.items():
                    self._link_asset_to_revision(
                        conn,
                        revision_id,
                        existing_asset,
                        required=asset_type in PLACE_REQUIRED_ASSET_TYPES,
                    )
                self._finalize_revision(
                    conn,
                    component_id=component_id,
                    revision_id=revision_id,
                    event_type="component.imported" if not existing else "revision.created",
                    actor=actor,
                    details={
                        "change_kind": "folder_import" if import_source == "folder_snapshot" else "project_import",
                        "change_summary": change_summary,
                        "proposal_id": proposal_id,
                        "provenance": provenance,
                    },
                )
            self._project_import_matching.record_component_usage(
                conn,
                component_id=component_id,
                provenance=provenance,
                observed_at=now,
                source="project_import",
            )
            self._project_import_acceptance.mark_proposal_accepted(
                conn,
                proposal_id,
                component_id,
                now=now,
            )
            conn.commit()
        return {
            "proposal": self.get_project_import_proposal(proposal_id),
            "component": self.get_component(component_id),
        }

    def reject_project_import_proposal(self, proposal_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._project_import_sessions.reject_proposal(
                conn,
                proposal_id,
                now=_utc_now_iso(),
            )
            conn.commit()
        return self.get_project_import_proposal(proposal_id) or {}

    def purge_superseded_step_files(self) -> dict[str, Any]:
        """Purge obsolete STEP bytes while preserving immutable revision evidence.

        The asset row, hash, revision link, and audit history remain intact. A file
        is removed only when a newer current revision for that component has a
        different 3D model and no component currently uses the old asset.
        """
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                WITH current_models AS (
                    SELECT revision.component_id, link.asset_id
                    FROM components component
                    JOIN component_revisions revision ON revision.id = component.current_revision_id
                    JOIN revision_assets link ON link.revision_id = revision.id
                    WHERE link.asset_type = '3dmodel'
                ), superseded_models AS (
                    SELECT DISTINCT revision.component_id, link.asset_id
                    FROM component_revisions revision
                    JOIN components component ON component.id = revision.component_id
                    JOIN revision_assets link ON link.revision_id = revision.id
                    JOIN current_models replacement
                      ON replacement.component_id = revision.component_id
                     AND replacement.asset_id <> link.asset_id
                    WHERE revision.id <> component.current_revision_id
                      AND link.asset_type = '3dmodel'
                )
                SELECT DISTINCT asset.id, asset.canonical_path
                FROM superseded_models superseded
                JOIN assets asset ON asset.id = superseded.asset_id
                LEFT JOIN current_models active ON active.asset_id = superseded.asset_id
                WHERE active.asset_id IS NULL
                  AND (
                    lower(asset.canonical_path) LIKE %s
                    OR lower(asset.canonical_path) LIKE %s
                  )
                """,
                ("%.step", "%.stp"),
            ).fetchall()
        purged: list[str] = []
        for row in rows:
            path = Path(str(row["canonical_path"] or "")).resolve()
            try:
                path.relative_to(self._store_root)
            except ValueError:
                continue
            if path.suffix.lower() not in {".step", ".stp"}:
                continue
            if path.is_file():
                path.unlink()
                purged.append(str(row["id"]))
        return {"purged": len(purged), "asset_ids": purged}

    def cleanup_resolved_import_staging(self, *, older_than: str) -> dict[str, Any]:
        """Remove regenerable staged copies after every proposal is resolved."""
        self.initialize()
        with self._connect() as conn:
            session_ids = self._project_import_sessions.list_resolved_session_ids(
                conn,
                older_than=older_than,
            )
        return self._project_import_sessions.remove_staging_directories(
            store_root=self._store_root,
            session_ids=session_ids,
        )

    def list_components(
        self,
        *,
        query: str = "",
        source: str | None = None,
        availability_state: str | None = None,
        workflow_stage: str | None = None,
        validation_status: str | None = None,
        category: str | None = None,
        include_inactive: bool = False,
        page: int = 1,
        page_size: int = 50,
        released_only: bool = False,
        lightweight: bool = False,
        sort_by: str = "",
        sort_dir: str = "asc",
    ) -> dict[str, Any]:
        self.initialize()
        plan = self._component_queries.prepare_list_components(
            query=query,
            source=source,
            availability_state=availability_state,
            workflow_stage=workflow_stage,
            validation_status=validation_status,
            category=category,
            include_inactive=include_inactive,
            page=page,
            page_size=page_size,
            released_only=released_only,
            lightweight=lightweight,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        with self._connect() as conn:
            return self._component_queries.execute_list_components(conn, plan)

    def list_components_flat(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.list_components(page=1, page_size=10000, **kwargs)["items"]

    def workflow_summary(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT cr.release_status AS workflow_stage, COUNT(1) AS count
                FROM components c
                JOIN component_revisions cr ON cr.id = c.current_revision_id
                WHERE c.is_active = 1
                GROUP BY cr.release_status
                """
            ).fetchall()
        counts = {stage: 0 for stage in WORKFLOW_STAGES}
        for row in rows:
            stage = _normalize_workflow_stage(str(row["workflow_stage"]))
            if stage in counts:
                counts[stage] += int(row["count"])
        return {"stages": [{"workflow_stage": stage, "count": counts[stage]} for stage in WORKFLOW_STAGES]}

    def release_queue_summary(self) -> dict[str, int]:
        """Return queue-wide counters without materializing component payloads.

        The release workspace is server paginated, so its header metrics must be
        computed independently from the visible page. A blocker is either missing
        required CAD or a failed validation run for the exact current revision.
        """

        self.initialize()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN cr.release_status = 'qa_review' THEN 1 ELSE 0 END) AS qa_review,
                    SUM(CASE WHEN cr.release_status = 'done' THEN 1 ELSE 0 END) AS done,
                    SUM(
                        CASE WHEN
                            NOT EXISTS (
                                SELECT 1 FROM revision_assets ra_symbol
                                WHERE ra_symbol.revision_id = cr.id
                                  AND ra_symbol.asset_type = 'symbol'
                            )
                            OR NOT EXISTS (
                                SELECT 1 FROM revision_assets ra_footprint
                                WHERE ra_footprint.revision_id = cr.id
                                  AND ra_footprint.asset_type = 'footprint'
                            )
                            OR EXISTS (
                                SELECT 1
                                FROM revision_assets ra_validation
                                JOIN assets validation_asset
                                  ON validation_asset.id = ra_validation.asset_id
                                WHERE ra_validation.revision_id = cr.id
                                  AND validation_asset.asset_type IN ('symbol', 'footprint')
                                  AND COALESCE((
                                      SELECT validation_run.status
                                      FROM asset_validation_runs validation_run
                                      WHERE validation_run.revision_id = cr.id
                                        AND validation_run.asset_id = validation_asset.id
                                      ORDER BY validation_run.finished_at DESC,
                                               validation_run.created_at DESC
                                      LIMIT 1
                                  ), 'not_run') = 'failed'
                            )
                        THEN 1 ELSE 0 END
                    ) AS blocked
                FROM components c
                JOIN component_revisions cr ON cr.id = c.current_revision_id
                WHERE c.is_active = 1
                  AND cr.release_status IN ('qa_review', 'done')
                """
            ).fetchone()
        return {
            "qa_review": int(row["qa_review"] or 0),
            "done": int(row["done"] or 0),
            "blocked": int(row["blocked"] or 0),
        }

    def search_components(self, query: str, *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        return self.list_components(
            query=query,
            include_inactive=False,
            page=page,
            page_size=page_size,
            released_only=True,
            lightweight=True,
        )

    def list_remote_component_heads(
        self,
        *,
        query: str = "",
        category: str | None = None,
        page: int = 1,
        page_size: int = 50,
        include_total: bool = True,
    ) -> dict[str, Any]:
        """Read the released KiCad-provider projection without hydrating revisions."""

        self.initialize()
        normalized_page = max(1, int(page))
        normalized_size = max(1, min(200, int(page_size)))
        offset = (normalized_page - 1) * normalized_size
        filters: list[str] = []
        params: list[Any] = []
        query_text = query.strip()
        if category is not None:
            filters.append("category = %s")
            params.append(category)
        if query_text:
            filters.append(
                "(LOWER(search_document) LIKE LOWER(%s) "
                "OR LOWER(mpn) LIKE LOWER(%s) "
                "OR LOWER(name) LIKE LOWER(%s))"
            )
            wildcard = f"%{query_text}%"
            params.extend([wildcard, wildcard, wildcard])
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        if query_text:
            order_sql = (
                "ORDER BY CASE "
                "WHEN LOWER(mpn) = LOWER(%s) THEN 0 "
                "WHEN LOWER(mpn) LIKE LOWER(%s) THEN 1 "
                "WHEN LOWER(name) LIKE LOWER(%s) THEN 2 "
                "ELSE 3 END, updated_at DESC"
            )
            order_params: list[Any] = [
                query_text,
                f"{query_text}%",
                f"{query_text}%",
            ]
        else:
            order_sql = "ORDER BY updated_at DESC"
            order_params = []

        with self._connect() as conn:
            total: int | None = None
            if include_total:
                total = int(
                    conn.execute(
                        f"SELECT COUNT(1) AS total FROM remote_component_heads {where_sql}",
                        tuple(params),
                    ).fetchone()["total"]
                )
            rows = conn.execute(
                f"""
                SELECT *
                FROM remote_component_heads
                {where_sql}
                {order_sql}
                LIMIT %s OFFSET %s
                """,
                tuple(params + order_params + [normalized_size + 1, offset]),
            ).fetchall()
            version_row = conn.execute(
                "SELECT value FROM catalog_meta "
                "WHERE key = 'remote_component_heads_version'"
            ).fetchone()

        has_more = len(rows) > normalized_size
        if has_more:
            rows = rows[:normalized_size]
        if total is not None:
            has_more = offset + len(rows) < total
        items: list[dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            has_symbol = bool(row.get("has_symbol"))
            has_footprint = bool(row.get("has_footprint"))
            missing_assets = [
                kind
                for kind, present in (
                    ("symbol", has_symbol),
                    ("footprint", has_footprint),
                )
                if not present
            ]
            if has_symbol and has_footprint:
                availability_state = STATE_PLACE_READY
            elif has_symbol or has_footprint:
                availability_state = STATE_FILES_PARTIAL
            else:
                availability_state = STATE_METADATA_ONLY
            assets: list[dict[str, Any]] = []
            if has_symbol:
                assets.append(
                    {
                        "asset_type": "symbol",
                        "target_library": str(row.get("symbol_library") or ""),
                        "target_name": str(row.get("symbol_name") or ""),
                    }
                )
            if has_footprint:
                assets.append({"asset_type": "footprint"})
            previews: list[dict[str, Any]] = []
            for kind in ("symbol", "footprint"):
                preview_id = str(row.get(f"{kind}_preview_id") or "")
                if preview_id:
                    previews.append(
                        {
                            "id": preview_id,
                            "kind": kind,
                            "status": PREVIEW_STATUS_READY,
                            "file_path": "projected",
                            "generation_error": "",
                        }
                    )
            items.append(
                {
                    "id": str(row["component_id"]),
                    "slug": str(row["slug"]),
                    "name": str(row["name"]),
                    "identity_kind": str(row.get("identity_kind") or IDENTITY_KIND_MPN),
                    "manufacturer": str(row["manufacturer"]),
                    "mpn": str(row["mpn"]),
                    "description": str(row["description"]),
                    "package_name": str(row["package_name"]),
                    "category": str(row["category"]),
                    "datasheet_url": str(row["datasheet_url"]),
                    "summary": str(row["summary"]),
                    "version": f"{int(row['version'])}.0.0",
                    "library_name": str(row.get("symbol_library") or ""),
                    "symbol_name": str(row.get("symbol_name") or ""),
                    "assets": assets,
                    "previews": previews,
                    "availability_state": availability_state,
                    "missing_assets": missing_assets,
                    "place_enabled": has_symbol and has_footprint and str(row.get("identity_kind") or IDENTITY_KIND_MPN) == IDENTITY_KIND_MPN,
                    "release_status": "released",
                    "workflow_stage": "released",
                    "supply": {
                        "sources": [
                            _supply_source_payload(source)
                            for source in _json_loads(row.get("inventory_sources"), [])
                        ]
                    },
                    "default_representation_id": str(row.get("default_representation_id") or ""),
                    "representation_count": int(row.get("representation_count") or 0),
                    "symbol_variant_count": int(row.get("symbol_variant_count") or 0),
                    "footprint_variant_count": int(row.get("footprint_variant_count") or 0),
                    "representations": [],
                    "extra_fields": _json_loads(row.get("extra_fields"), {}),
                }
            )
        return {
            "items": items,
            "total": total,
            "has_more": has_more,
            "page": normalized_page,
            "page_size": normalized_size,
            "pages": (
                max(1, (total + normalized_size - 1) // normalized_size)
                if total is not None
                else None
            ),
            "projection_version": (
                str(version_row["value"])
                if version_row
                else "0"
            ),
        }

    def list_remote_categories(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT category AS name, COUNT(1) AS count
                FROM remote_component_heads
                GROUP BY category
                ORDER BY category
                """
            ).fetchall()
            version_row = conn.execute(
                "SELECT value FROM catalog_meta "
                "WHERE key = 'remote_component_heads_version'"
            ).fetchone()
        return {
            "categories": [
                {
                    "name": str(row["name"] or ""),
                    "count": int(row["count"]),
                }
                for row in rows
            ],
            "projection_version": (
                str(version_row["value"])
                if version_row
                else "0"
            ),
        }

    def remote_projection_version(self) -> str:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM catalog_meta "
                "WHERE key = 'remote_component_heads_version'"
            ).fetchone()
        return str(row["value"]) if row else "0"

    def list_categories(self) -> list[dict[str, Any]]:
        self.initialize()
        now = time.monotonic()
        runtime = self._catalog_runtime
        if runtime.category_cache is not None and (now - runtime.category_cache_ts) < runtime.category_cache_ttl:
            return runtime.category_cache
        with self._connect() as conn:
            result = self._component_queries.list_categories(conn)
        runtime.category_cache = result
        runtime.category_cache_ts = now
        return result

    def get_component(
        self,
        component_id: str,
        *,
        include_inactive: bool = True,
        released_only: bool = False,
        representation_id: str = "",
    ) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._component_read_models.get_component(
                conn,
                component_id,
                include_inactive=include_inactive,
                released_only=released_only,
                representation_id=representation_id,
            )

    def _representation_asset_id(
        self, conn: Any, revision_id: str, asset_id: str, expected_type: str
    ) -> str | None:
        return self._representations.representation_asset_id(conn, revision_id, asset_id, expected_type)

    def create_representation(
        self,
        component_id: str,
        *,
        label: str,
        symbol_asset_id: str = "",
        footprint_asset_id: str = "",
        display_order: int = 0,
        make_default: bool = False,
        source_internal_part_number: str = "",
        provenance: dict[str, Any] | None = None,
        expected_revision_id: str,
        actor: str = "",
        change_summary: str = "Add component representation",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._representations.create_representation(
                conn,
                self._runtime_for_compat(),
                component_id,
                label=label,
                symbol_asset_id=symbol_asset_id,
                footprint_asset_id=footprint_asset_id,
                display_order=display_order,
                make_default=make_default,
                source_internal_part_number=source_internal_part_number,
                provenance=provenance,
                expected_revision_id=expected_revision_id,
                actor=actor,
                change_summary=change_summary,
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def _current_representation_row(
        self, conn: Any, component_id: str, representation_id: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return self._representations.current_representation_row(conn, component_id, representation_id)

    def update_representation(
        self,
        component_id: str,
        representation_id: str,
        *,
        updates: dict[str, Any],
        expected_revision_id: str,
        actor: str = "",
        change_summary: str = "Update component representation",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._representations.update_representation(
                conn,
                self._runtime_for_compat(),
                component_id,
                representation_id,
                updates=updates,
                expected_revision_id=expected_revision_id,
                actor=actor,
                change_summary=change_summary,
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def delete_representation(
        self,
        component_id: str,
        representation_id: str,
        *,
        expected_revision_id: str,
        actor: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._representations.delete_representation(
                conn,
                self._runtime_for_compat(),
                component_id,
                representation_id,
                expected_revision_id=expected_revision_id,
                actor=actor,
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def create_manual_component(self, *, actor: str = "", change_summary: str = "Create component", **payload: Any) -> dict[str, Any]:
        self.initialize()
        metadata = normalize_metadata(payload)
        now = _utc_now_iso()
        component_id = str(uuid.uuid4())
        with self._connect() as conn:
            self._upsert_component_metadata_row(
                conn,
                component_id=component_id,
                metadata=metadata,
                now=now,
                existing_component_id=None,
                actor=actor,
                change_summary=change_summary,
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def _upsert_component_metadata_row(
        self,
        conn: Any,
        *,
        component_id: str,
        metadata: dict[str, Any],
        now: str,
        existing_component_id: str | None,
        actor: str = "",
        change_summary: str = "Update component metadata",
        expected_revision_id: str = "",
        finalize_revision: bool = True,
        source: str = SOURCE_MANUAL,
        external_source: str = "",
        external_id: str = "",
        change_kind: str = "metadata",
    ) -> tuple[str, str]:
        self._metadata_schema.ensure_extra_field_definitions(
            conn,
            metadata.get("extra_fields", {}).keys(),
            actor=actor or "system:catalog",
        )
        self._assert_component_identity_available(
            conn,
            manufacturer=metadata["manufacturer"],
            mpn=metadata["mpn"],
            identity_kind=metadata["identity_kind"],
            identity_source=metadata["identity_source"],
            source_internal_part_number=metadata["source_internal_part_number"],
            component_id=existing_component_id or "",
        )
        if existing_component_id:
            revision = self._clone_revision(
                conn,
                existing_component_id,
                actor=actor,
                change_kind=change_kind,
                change_summary=change_summary,
                expected_revision_id=expected_revision_id,
            )
            conn.execute(
                """
                UPDATE component_revisions
                SET name = %s, value = %s, description = %s, datasheet_url = %s, manufacturer = %s, mpn = %s,
                    normalized_manufacturer = %s, normalized_mpn = %s, mpn_source = %s,
                    category = %s, package_name = %s, vendor = %s, vendor_part_number = %s, mass_g = %s,
                    rqjc_c_w = %s, rqjc_top_c_w = %s, temp_max_c = %s, temp_min_c = %s,
                    power_dissipation_w = %s, rate = %s, sap_code = %s, summary = %s, keywords = %s, extra_fields = %s,
                    search_document = %s, updated_at = %s
                WHERE id = %s
                """,
                (
                    metadata["name"],
                    metadata["value"],
                    metadata["description"],
                    metadata["datasheet_url"],
                    metadata["manufacturer"],
                    metadata["mpn"],
                    metadata["normalized_manufacturer"],
                    metadata["normalized_mpn"],
                    metadata["mpn_source"],
                    metadata["category"],
                    metadata["package_name"],
                    metadata["vendor"],
                    metadata["vendor_part_number"],
                    metadata["mass_g"],
                    metadata["rqjc_c_w"],
                    metadata["rqjc_top_c_w"],
                    metadata["temp_max_c"],
                    metadata["temp_min_c"],
                    metadata["power_dissipation_w"],
                    metadata["rate"],
                    metadata["sap_code"],
                    metadata["summary"],
                    json.dumps(metadata_keywords(metadata), separators=(",", ":")),
                    json.dumps(metadata["extra_fields"], sort_keys=True, separators=(",", ":")),
                    metadata_search_document(metadata),
                    now,
                    revision["id"],
                ),
            )
            conn.execute(
                """
                UPDATE components
                SET identity_kind = %s, identity_source = %s,
                    normalized_manufacturer = %s, normalized_part_number = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    metadata["identity_kind"],
                    metadata["identity_source"] if metadata["identity_kind"] == IDENTITY_KIND_PROVISIONAL_IPN else "",
                    metadata["normalized_manufacturer"] if metadata["identity_kind"] == IDENTITY_KIND_MPN else "",
                    metadata["normalized_part_number"],
                    now,
                    existing_component_id,
                ),
            )
            if finalize_revision:
                self._finalize_revision(
                    conn,
                    component_id=existing_component_id,
                    revision_id=str(revision["id"]),
                    event_type="revision.created",
                    actor=actor,
                    details={"change_kind": change_kind, "change_summary": change_summary},
                )
            return existing_component_id, str(revision["id"])

        slug = self._unique_slug(
            conn,
            metadata["mpn"] or metadata["source_internal_part_number"] or metadata["value"],
        )
        revision_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO components (
                id, slug, identity_kind, identity_source, normalized_manufacturer, normalized_part_number,
                source, external_source, external_id, is_active, current_revision_id,
                released_revision_id, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, '', %s, %s)
            """,
            (
                component_id,
                slug,
                metadata["identity_kind"],
                metadata["identity_source"] if metadata["identity_kind"] == IDENTITY_KIND_PROVISIONAL_IPN else "",
                metadata["normalized_manufacturer"] if metadata["identity_kind"] == IDENTITY_KIND_MPN else "",
                metadata["normalized_part_number"],
                source,
                external_source,
                external_id,
                revision_id,
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO component_revisions (
                id, component_id, version, parent_revision_id, change_kind, change_summary, created_by,
                manifest_hash, manifest_schema, release_status, name, value, description, datasheet_url,
                manufacturer, mpn, normalized_manufacturer, normalized_mpn, mpn_source,
                category, package_name, vendor, vendor_part_number, mass_g,
                rqjc_c_w, rqjc_top_c_w, temp_max_c, temp_min_c, power_dissipation_w, rate, sap_code,
                summary, keywords, extra_fields, search_document, created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                revision_id,
                component_id,
                1,
                "",
                "create" if source == SOURCE_MANUAL else change_kind,
                change_summary,
                actor,
                "",
                REVISION_MANIFEST_A3,
                "open",
                metadata["name"],
                metadata["value"],
                metadata["description"],
                metadata["datasheet_url"],
                metadata["manufacturer"],
                metadata["mpn"],
                metadata["normalized_manufacturer"],
                metadata["normalized_mpn"],
                metadata["mpn_source"],
                metadata["category"],
                metadata["package_name"],
                metadata["vendor"],
                metadata["vendor_part_number"],
                metadata["mass_g"],
                metadata["rqjc_c_w"],
                metadata["rqjc_top_c_w"],
                metadata["temp_max_c"],
                metadata["temp_min_c"],
                metadata["power_dissipation_w"],
                metadata["rate"],
                metadata["sap_code"],
                metadata["summary"],
                json.dumps(metadata_keywords(metadata), separators=(",", ":")),
                json.dumps(metadata["extra_fields"], sort_keys=True, separators=(",", ":")),
                metadata_search_document(metadata),
                now,
                now,
            ),
        )
        if finalize_revision:
            self._finalize_revision(
                conn,
                component_id=component_id,
                revision_id=revision_id,
                event_type="component.created",
                actor=actor,
                details={"change_kind": "create", "change_summary": change_summary},
            )
        return component_id, revision_id

    def update_component_metadata(
        self,
        component_id: str,
        updates: dict[str, Any],
        *,
        actor: str = "",
        change_summary: str = "Update component metadata",
        expected_revision_id: str = "",
    ) -> dict[str, Any] | None:
        if not expected_revision_id.strip():
            raise ValueError("expected_revision_id is required when updating component metadata")
        self.initialize()
        with self._connect() as conn:
            component = self._component_row(conn, component_id)
            if not component:
                return None
            _, revision = self._active_revision_row(conn, component_id, released=False)
            if not revision:
                return None
            # Keep advisory-lock ordering consistent with project imports and creates:
            # identity first, component row second. Once the component lock is held,
            # reload the head before merging any client patch.
            target_manufacturer = str(updates.get("manufacturer", revision.get("manufacturer") or ""))
            target_mpn = str(updates.get("mpn", revision.get("mpn") or ""))
            self._lock_component_identity(conn, target_manufacturer, target_mpn)
            self._lock_component_for_mutation(conn, component_id)
            component = self._component_row(conn, component_id)
            if not component:
                return None
            _, revision = self._active_revision_row(conn, component_id, released=False)
            if not revision:
                return None
            if str(revision["id"]) != expected_revision_id:
                raise ValueError("Component revision conflict: refresh the component before saving")
            merged = {**revision}
            merged["identity_kind"] = str(component.get("identity_kind") or IDENTITY_KIND_MPN)
            merged["identity_source"] = str(component.get("identity_source") or "")
            if merged["identity_kind"] == IDENTITY_KIND_PROVISIONAL_IPN:
                merged["source_internal_part_number"] = str(component.get("normalized_part_number") or "")
                if target_mpn.strip():
                    merged["identity_kind"] = IDENTITY_KIND_MPN
                    merged["identity_source"] = ""
            merged["extra_fields"] = _json_loads(revision.get("extra_fields"), {})
            field_map = {
                "datasheet_url": "datasheet_url",
                "mpn": "mpn",
                "value": "value",
                "description": "description",
                "manufacturer": "manufacturer",
                "category": "category",
                "package_name": "package_name",
                "vendor": "vendor",
                "vendor_part_number": "vendor_part_number",
                "mass_g": "mass_g",
                "rqjc_c_w": "rqjc_c_w",
                "rqjc_top_c_w": "rqjc_top_c_w",
                "temp_max_c": "temp_max_c",
                "temp_min_c": "temp_min_c",
                "power_dissipation_w": "power_dissipation_w",
                "rate": "rate",
                "sap_code": "sap_code",
            }
            for key, column in field_map.items():
                if key in updates:
                    merged[column] = str(updates[key] or "")
            if "extra_fields" in updates:
                merged["extra_fields"] = dict(updates["extra_fields"] or {})
            metadata = normalize_metadata(merged)
            unchanged = all(
                (
                    _json_loads(revision.get(key), {}) == metadata[key]
                    if key == "extra_fields"
                    else str(revision.get(key) or "") == str(metadata[key])
                )
                for key in metadata
            )
            if unchanged:
                return self.get_component(component_id)
            now = _utc_now_iso()
            self._upsert_component_metadata_row(
                conn,
                component_id=component_id,
                metadata=metadata,
                now=now,
                existing_component_id=component_id,
                actor=actor,
                change_summary=change_summary,
                expected_revision_id=expected_revision_id,
            )
            conn.commit()
        return self.get_component(component_id)

    # ── Metadata field registry and auditable bulk editing ──────────────────

    def list_metadata_fields(self, *, include_archived: bool = False) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            return self._metadata_fields.list_fields(conn, include_archived=include_archived)

    def create_metadata_field(self, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        self.initialize()
        prepared = self._metadata_fields.prepare_create_field(payload)
        with self._connect() as conn:
            after = self._metadata_fields.create_field(conn, prepared, actor)
            conn.commit()
        return after

    def update_metadata_field(self, field_id: str, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            after = self._metadata_fields.update_field(conn, field_id, payload, actor)
            conn.commit()
        return after

    def set_metadata_field_archived(self, field_id: str, archived: bool, *, actor: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            after = self._metadata_fields.set_field_archived(conn, field_id, archived, actor)
            conn.commit()
        return after

    def get_metadata_grid_preferences(self, user_email: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._metadata_grid.get_preferences(conn, user_email)

    def save_metadata_grid_preferences(self, user_email: str, layout: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        prepared = self._metadata_grid.prepare_preferences(user_email, layout)
        with self._connect() as conn:
            self._metadata_grid.save_preferences(conn, prepared)
            conn.commit()
        return prepared.layout

    def metadata_grid(self, *, field_keys: list[str] | None = None, **filters: Any) -> dict[str, Any]:
        fields = self.list_metadata_fields()
        if field_keys is not None:
            requested = {str(key) for key in field_keys}
            fields = [field for field in fields if str(field["key"]) in requested]
        result = self.list_components(lightweight=True, include_inactive=False, **filters)
        component_ids = [str(item["id"]) for item in result["items"]]
        if component_ids:
            prepared = self._metadata_grid.prepare_rows(component_ids, fields)
            with self._connect() as conn:
                rows = self._metadata_grid.fetch_rows(conn, prepared)
            self._metadata_grid.hydrate_rows(prepared, rows, result["items"])
        return {**result, "schema": METADATA_SCHEMA_VERSION, "fields": fields}

    def get_metadata_batch(self, batch_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._metadata_batches.batch_payload(conn, batch_id)

    def stage_metadata_batch(
        self,
        items: list[dict[str, Any]],
        *,
        source: str,
        actor: str,
        change_summary: str,
        proposed_fields: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        component_ids = [str(item.get("component_id") or "") for item in items]
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("Each component may appear only once in a metadata batch")
        batch_id = str(uuid.uuid4())
        now = _utc_now_iso()
        fields = {field["key"]: field for field in self.list_metadata_fields()}
        for proposal in proposed_fields or []:
            fields[str(proposal["key"])] = {**proposal, "storage_kind": "extra", "storage_key": proposal["key"], "archived": False}
        valid_count = 0
        with self._connect() as conn:
            duplicate_identities = self._metadata_batch_staging.duplicate_identities(conn, items)
            self._metadata_batches.insert_batch(
                conn,
                batch_id=batch_id,
                source=source,
                status="needs_fields" if proposed_fields else "ready",
                schema_version=METADATA_SCHEMA_VERSION,
                change_summary=change_summary.strip() or "Bulk update component metadata",
                unknown_fields_json=json.dumps(proposed_fields or [], separators=(",", ":")),
                created_by=actor,
                total_items=len(items),
                created_at=now,
                updated_at=now,
            )
            for raw_item in items:
                preparation = self._metadata_batch_staging.prepare_item(
                    conn, raw_item, fields, duplicate_identities
                )
                component_id = preparation.component_id
                expected_revision_id = preparation.expected_revision_id
                normalized_patch = preparation.normalized_patch
                diff = preparation.diff
                errors = list(preparation.errors)
                if preparation.target_identity is not None:
                    target_manufacturer, target_mpn, target_name = preparation.target_identity
                    try:
                        self._assert_component_identity_available(
                            conn,
                            manufacturer=target_manufacturer,
                            mpn=target_mpn,
                            name=target_name,
                            component_id=component_id,
                            acquire_identity_lock=False,
                        )
                    except ValueError as exc:
                        errors.append(str(exc))
                status = "invalid" if errors else "valid" if diff else "noop"
                if status == "valid":
                    valid_count += 1
                self._metadata_batches.insert_batch_item(
                    conn,
                    item_id=str(uuid.uuid4()),
                    batch_id=batch_id,
                    component_id=component_id,
                    expected_revision_id=expected_revision_id,
                    patch_json=json.dumps(normalized_patch, sort_keys=True, separators=(",", ":")),
                    diff_json=json.dumps(diff, separators=(",", ":")),
                    validation_status=status,
                    error_message="; ".join(errors),
                    created_at=now,
                    updated_at=now,
                )
            self._metadata_batches.update_valid_items(conn, batch_id, valid_count)
            conn.commit()
            return self._metadata_batches.batch_payload(conn, batch_id) or {}

    def approve_metadata_batch_fields(self, batch_id: str, *, actor: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            proposals = self._metadata_batches.fetch_batch_field_proposals(conn, batch_id)
            if proposals is None:
                raise ValueError("Metadata batch not found")
        for proposal in proposals:
            try:
                self.create_metadata_field(proposal, actor=actor)
            except ValueError as exc:
                if "already exists" not in str(exc):
                    raise
        with self._connect() as conn:
            self._metadata_batches.mark_fields_approved(
                conn,
                batch_id,
                _utc_now_iso(),
            )
            conn.commit()
            return self._metadata_batches.batch_payload(conn, batch_id) or {}

    def _inherit_validation_evidence(self, conn: Any, parent_revision_id: str, revision_id: str) -> None:
        return self._revision_kernel.inherit_validation_evidence(
            conn,
            parent_revision_id,
            revision_id,
        )

    def apply_metadata_batch_item(self, item_id: str, *, actor: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            item = self._metadata_batches.fetch_item_for_apply(conn, item_id)
            early_result = self._metadata_batch_application.classify_item(item_id, item)
            if early_result is not None:
                return early_result
            component_id = str(item["component_id"])
            self._lock_component_for_mutation(conn, component_id)
            component, revision = self._active_revision_row(conn, component_id, released=False)
            if not component or not revision:
                raise ValueError("Component not found")
            if str(revision["id"]) != str(item["expected_revision_id"]):
                raise ValueError("Component revision conflict: current revision changed after preview")
            definitions = {field["key"]: field for field in self.list_metadata_fields()}
            prepared = self._metadata_batch_application.prepare_revision(item, revision, definitions)
            metadata = prepared.metadata
            self._lock_component_identity(conn, metadata["manufacturer"], metadata["mpn"])
            _, revision_id = self._upsert_component_metadata_row(
                conn,
                component_id=component_id,
                metadata=metadata,
                now=_utc_now_iso(),
                existing_component_id=component_id,
                actor=actor,
                change_summary=prepared.change_summary,
                expected_revision_id=prepared.parent_revision_id,
                finalize_revision=False,
                change_kind="metadata_bulk",
            )
            conn.execute("UPDATE component_revisions SET release_status = 'qa_review' WHERE id = %s", (revision_id,))
            self._inherit_validation_evidence(conn, prepared.parent_revision_id, revision_id)
            self._finalize_revision(
                conn,
                component_id=component_id,
                revision_id=revision_id,
                event_type="revision.created",
                actor=actor,
                details=prepared.finalize_details,
            )
            self._metadata_batches.mark_item_applied(
                conn,
                item_id,
                revision_id,
                _utc_now_iso(),
            )
            conn.commit()
        return self._metadata_batch_application.applied_result(item_id, revision_id)

    def apply_metadata_batch(
        self,
        batch_id: str,
        *,
        actor: str,
        item_ids: list[str] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            batch = self._metadata_batches.fetch_batch_for_apply(conn, batch_id)
            if not batch:
                raise ValueError("Metadata batch not found")
            if str(batch["status"]) == "needs_fields":
                raise ValueError("Unknown CSV fields must be approved before applying")
            rows = self._metadata_batches.fetch_valid_item_rows(conn, batch_id)
        selection = self._metadata_batch_application.select_valid_item_ids(rows, item_ids)
        ids = list(selection.ids)
        if selection.selected and not ids:
            raise ValueError("None of the selected metadata batch items are valid")
        applied = 0
        failed = 0
        errors: list[dict[str, str]] = []
        for index, item_id in enumerate(ids):
            try:
                self.apply_metadata_batch_item(item_id, actor=actor)
                applied += 1
            except ValueError as exc:
                failed += 1
                errors.append({"item_id": item_id, "error": str(exc)})
                with self._connect() as conn:
                    self._metadata_batches.mark_item_conflict(
                        conn,
                        item_id,
                        str(exc),
                        _utc_now_iso(),
                    )
                    conn.commit()
            if progress_callback:
                progress_callback({"completed": index + 1, "total": len(ids), "applied": applied, "failed": failed})
        with self._connect() as conn:
            totals = self._metadata_batches.calculate_batch_totals(conn, batch_id)
            accounting = self._metadata_batch_application.account_batch(
                batch_id, totals, applied, failed, errors
            )
            self._metadata_batches.finalize_batch(
                conn,
                batch_id=batch_id,
                status=accounting.status,
                valid_items=accounting.remaining,
                applied_items=accounting.total_applied,
                failed_items=accounting.total_failed,
                updated_at=_utc_now_iso(),
            )
            conn.commit()
        return accounting.result

    def export_metadata_csv(self, field_keys: list[str] | None = None) -> str:
        return "".join(self.iter_metadata_csv(field_keys=field_keys))

    def iter_metadata_csv(self, field_keys: list[str] | None = None) -> Iterator[str]:
        self.initialize()
        fields = self.list_metadata_fields()
        prepared = self._metadata_csv.prepare_export(
            fields,
            field_keys,
            schema_version=METADATA_SCHEMA_VERSION,
        )

        def generate() -> Iterator[str]:
            yield self._metadata_csv.render_header(prepared)

            with self._connect() as conn:
                sql = (
                    "SELECT c.id AS component_id, cr.* FROM components c "
                    "JOIN component_revisions cr ON cr.id = c.current_revision_id "
                    "WHERE c.is_active = 1 ORDER BY cr.manufacturer, cr.mpn, c.id"
                )
                if hasattr(conn, "iter_rows"):
                    rows = conn.iter_rows(sql, batch_size=500)
                else:
                    rows = iter(conn.execute(sql).fetchall())
                for row in rows:
                    yield self._metadata_csv.render_row(prepared, row)

        return generate()

    def preview_metadata_csv(self, file_content: str, *, actor: str, change_summary: str = "Import component metadata from CSV") -> dict[str, Any]:
        self.initialize()
        fields = {field["key"]: field for field in self.list_metadata_fields(include_archived=True)}
        parsed = self._metadata_csv.parse_preview(file_content, list(fields.values()))

        with self._connect() as conn:
            current_rows = {
                str(row["component_id"]): dict(row)
                for row in conn.execute(
                    "SELECT c.id AS component_id, cr.* FROM components c "
                    "JOIN component_revisions cr ON cr.id = c.current_revision_id WHERE c.is_active = 1"
                ).fetchall()
            }

        changes = self._metadata_csv.filter_preview_changes(
            parsed.parsed_rows,
            current_rows,
            fields,
            parsed.proposed_fields,
        )
        batch = self.stage_metadata_batch(
            changes.items,
            source="csv",
            actor=actor,
            change_summary=change_summary,
            proposed_fields=changes.used_proposals,
        )
        return {
            **batch,
            "source_rows": len(parsed.parsed_rows),
            "skipped_unchanged_rows": changes.skipped_unchanged_rows,
        }

    def import_metadata_csv(self, file_content: str) -> dict[str, Any]:
        self.initialize()
        parsed = self._metadata_csv.parse_import(file_content)

        created = 0
        updated = 0
        with self._connect() as conn:
            now = _utc_now_iso()
            for prepared_row in parsed.rows:
                row = prepared_row.row
                mpn = row["manufacturer_part_number"]
                existing = conn.execute(
                    """
                    SELECT c.id
                    FROM components c
                    JOIN component_revisions cr ON cr.id = c.current_revision_id
                    WHERE cr.mpn = %s
                    LIMIT 1
                    """,
                    (mpn,),
                ).fetchone()
                normalized = normalize_metadata(prepared_row.payload)
                if existing:
                    component_id, revision_id = self._upsert_component_metadata_row(
                        conn,
                        component_id=str(existing["id"]),
                        metadata=normalized,
                        now=now,
                        existing_component_id=str(existing["id"]),
                        actor="csv-import",
                        finalize_revision=False,
                    )
                    updated += 1
                else:
                    component_id = str(uuid.uuid4())
                    component_id, revision_id = self._upsert_component_metadata_row(
                        conn,
                        component_id=component_id,
                        metadata=normalized,
                        now=now,
                        existing_component_id=None,
                        actor="csv-import",
                        finalize_revision=False,
                    )
                    created += 1

                for asset_type, file_path, target_library, target_name in prepared_row.asset_links:
                    asset = self._resolve_existing_asset(
                        conn,
                        asset_type=asset_type,
                        file_path=file_path,
                        target_library=target_library,
                        target_name=target_name,
                    )
                    self._link_asset_to_revision(conn, revision_id, asset, required=asset_type in PLACE_REQUIRED_ASSET_TYPES)
                self._finalize_revision(
                    conn,
                    component_id=component_id,
                    revision_id=revision_id,
                    event_type="revision.created" if existing else "component.created",
                    actor="csv-import",
                    details={
                        "change_kind": "csv_import",
                        "change_summary": "Import component metadata and assets from CSV",
                    },
                )
            conn.commit()
        return {"created": created, "updated": updated, "errors": []}

    def export_inventory_csv(self) -> str:
        self.initialize()
        with self._connect() as conn:
            rows = self._inventory_csv.fetch_export_rows(conn)
        return self._inventory_csv.render_export(rows)

    def import_inventory_csv(self, file_content: str) -> dict[str, Any]:
        self.initialize()
        reader = self._inventory_csv.parse(file_content)
        updated = 0
        not_found = 0
        errors: list[str] = []
        with self._connect() as conn:
            for index, row in enumerate(reader, start=2):
                try:
                    identity = self._inventory_csv.prepare_identity(row, index)
                except ValueError as exc:
                    errors.append(str(exc))
                    continue
                component = self._inventory_csv.find_component(conn, identity)
                if not component:
                    not_found += 1
                    errors.append(f"Row {index}: component identity was not found")
                    continue
                try:
                    self._inventory_csv.validate_component(component, identity, index)
                    prepared = self._inventory_csv.prepare_upsert(row, index)
                except ValueError as exc:
                    errors.append(str(exc))
                    continue
                now = _utc_now_iso()
                self._inventory_csv.upsert(conn, component["id"], index, prepared, now)
                updated += 1
            conn.commit()
        return {"updated": updated, "not_found": not_found, "errors": errors}

    def _invalidate_browse_cache(self) -> None:
        """Drop the stored-file listings after the store on disk changes.

        Called from the one place bytes land in the store, so the asset picker
        never offers a file that has just been rewritten elsewhere, nor hides
        one that has just arrived. Clearing every type is deliberate: the
        listings are rebuilt lazily on the next browse, and a write does not
        reliably tell us which tree it touched.
        """
        self._runtime_for_compat().invalidate_browse_cache()

    def browse_library_assets(
        self,
        asset_type: str,
        q: str = "",
        limit: int = 100,
    ) -> dict[str, Any]:
        """List stored asset files without walking the store on every request.

        The recursive walk over the symbol/footprint trees is expensive once
        libraries hold thousands of files, so the sorted listing is cached per
        asset type and reused across requests. ``q`` filters the cached listing
        and ``limit`` bounds the response so the picker never renders the whole
        store. Writes into the store drop the cache, so the TTL is a backstop
        for changes made outside this process rather than the freshness bound.
        """
        self.initialize()
        root = self._asset_root(asset_type)
        return self._asset_browser.browse(
            runtime=self._catalog_runtime,
            root=root,
            asset_type=asset_type,
            q=q,
            limit=limit,
            now=time.monotonic(),
        )

    def _attach_asset_revision(
        self,
        conn: Any,
        *,
        component_id: str,
        asset: dict[str, Any],
        required: bool,
        actor: str,
        change_summary: str,
        counterpart_asset_id: str = "",
    ) -> dict[str, Any]:
        return self._asset_links.attach_asset_revision(
            conn,
            self._runtime_for_compat(),
            component_id=component_id,
            asset=asset,
            required=required,
            actor=actor,
            change_summary=change_summary,
            counterpart_asset_id=counterpart_asset_id,
        )

    def _extract_top_level_symbol_blocks(self, text: str) -> list[tuple[str, str]]:
        return self._asset_files.extract_top_level_symbol_blocks(text)

    def _symbol_header(self, text: str) -> tuple[str, str]:
        return self._asset_files.symbol_header(text)

    def _single_symbol_payload(self, text: str, selected_symbol: str) -> bytes:
        return self._asset_files.single_symbol_payload(text, selected_symbol)

    def _write_canonical_file(self, destination: Path, payload: bytes) -> Path:
        return self._asset_files.write_canonical_file(
            self._runtime_for_compat(), destination, payload
        )

    def _symbol_destination(self, target_library: str, target_name: str) -> Path:
        return self._asset_files.symbol_destination(
            self._runtime_for_compat(), target_library, target_name
        )

    def _footprint_destination(self, target_library: str, target_name: str) -> Path:
        return self._asset_files.footprint_destination(
            self._runtime_for_compat(), target_library, target_name
        )

    def _aux_destination(self, asset_type: str, target_library: str, upload_name: str) -> Path:
        return self._asset_files.aux_destination(
            self._runtime_for_compat(), asset_type, target_library, upload_name
        )

    def _asset_by_key(self, conn: Any, asset_type: str, canonical_path: str, target_name: str) -> dict[str, Any] | None:
        return self._asset_registry.asset_by_key(
            conn, asset_type, canonical_path, target_name
        )

    def _asset_by_signature(
        self,
        conn: Any,
        asset_type: str,
        sha256: str,
        target_library: str,
        target_name: str,
    ) -> dict[str, Any] | None:
        return self._asset_registry.asset_by_signature(
            conn, asset_type, sha256, target_library, target_name
        )

    def _register_asset(
        self,
        conn: Any,
        *,
        asset_type: str,
        canonical_path: Path,
        target_library: str,
        target_name: str,
        source_group: str = "",
    ) -> dict[str, Any]:
        return self._asset_registry.register_asset(
            self._runtime_for_compat(),
            conn,
            asset_type=asset_type,
            canonical_path=canonical_path,
            target_library=target_library,
            target_name=target_name,
            source_group=source_group,
        )

    def _generate_symbol_preview(self, asset: dict[str, Any]) -> tuple[str, bytes | str]:
        """Compatibility single-unit renderer used by existing test/custom adapters."""
        return self._preview_renderer.generate_symbol_preview(asset, self._run_kicad_cli)

    def _generate_symbol_preview_units(
        self,
        asset: dict[str, Any],
    ) -> tuple[str, list[tuple[int, bytes]] | str]:
        return self._preview_renderer.generate_symbol_preview_units(asset, self._run_kicad_cli)

    def _generate_footprint_preview(self, asset: dict[str, Any]) -> tuple[str, bytes | str]:
        return self._preview_renderer.generate_footprint_preview(asset, self._run_kicad_cli)

    def _store_preview_version(
        self,
        conn: Any,
        *,
        asset: dict[str, Any],
        kind: str,
        payload: bytes,
    ) -> dict[str, Any]:
        return self._preview_pipeline.store_preview_version(
            conn, self._runtime_for_compat(), asset=asset, kind=kind, payload=payload
        )

    def _ensure_asset_previews(self, conn: Any, asset: dict[str, Any]) -> list[dict[str, Any]]:
        return self._preview_pipeline.ensure_asset_previews(conn, self._runtime_for_compat(), asset)

    def _ensure_asset_preview(self, conn: Any, asset: dict[str, Any]) -> dict[str, Any]:
        """Compatibility wrapper returning the first generated preview."""
        previews = self._ensure_asset_previews(conn, asset)
        return previews[0] if previews else {}

    def _has_ready_preview(self, conn: Any, asset_id: str, kind: str) -> bool:
        return self._preview_pipeline.has_ready_preview(conn, self._runtime_for_compat(), asset_id, kind)

    def _refresh_revision_preview_outputs_in_conn(
        self,
        conn: Any,
        revision_id: str,
        *,
        only_missing: bool = False,
    ) -> dict[str, Any]:
        return self._preview_pipeline.refresh_revision_preview_outputs(
            conn, self._runtime_for_compat(), revision_id, only_missing=only_missing
        )

    def _regenerate_component_previews_in_conn(
        self,
        conn: Any,
        component_id: str,
        *,
        actor: str,
        only_missing: bool = False,
    ) -> dict[str, Any]:
        _ = actor
        return self._preview_pipeline.regenerate_component_previews(
            conn, self._runtime_for_compat(), component_id, only_missing=only_missing
        )

    def generate_missing_component_previews(
        self,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._preview_pipeline.generate_missing_component_previews(
                conn, self._runtime_for_compat(), progress_callback
            )

    def _link_asset_to_revision(
        self,
        conn: Any,
        revision_id: str,
        asset: dict[str, Any],
        *,
        required: bool,
        counterpart_asset_id: str = "",
    ) -> None:
        self._asset_links.link_asset_to_revision(
            conn, revision_id, asset, required=required, counterpart_asset_id=counterpart_asset_id
        )

    def _resolve_existing_asset(
        self,
        conn: Any,
        *,
        asset_type: str,
        file_path: str,
        target_library: str,
        target_name: str,
    ) -> dict[str, Any]:
        return self._asset_imports.resolve_existing_asset(
            conn,
            self._runtime_for_compat(),
            asset_type=asset_type,
            file_path=file_path,
            target_library=target_library,
            target_name=target_name,
        )

    def link_library_asset(
        self,
        component_id: str,
        asset_type: str,
        file_path_rel: str,
        target_library: str,
        target_name: str,
        *,
        counterpart_asset_id: str = "",
        actor: str = "",
    ) -> dict[str, Any]:
        if asset_type not in SUPPORTED_ASSET_TYPES:
            raise ValueError("Unsupported asset type")
        self.initialize()
        with self._connect() as conn:
            self._asset_imports.link_library_asset(
                conn,
                self._runtime_for_compat(),
                component_id,
                asset_type,
                file_path_rel,
                target_library,
                target_name,
                counterpart_asset_id=counterpart_asset_id,
                actor=actor,
            )
            conn.commit()
        return {"component": self.get_component(component_id)}

    def _normalize_symbol_upload(self, upload_name: str, payload: bytes) -> bytes:
        return self._asset_imports.normalize_symbol_upload(
            self._runtime_for_compat(), upload_name, payload
        )

    def import_symbol_library(
        self,
        component_id: str,
        *,
        upload_name: str,
        payload: bytes,
        target_library: str,
        selected_symbol: str,
        counterpart_asset_id: str = "",
        actor: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            result = self._asset_imports.import_symbol_library(
                conn,
                self._runtime_for_compat(),
                component_id,
                upload_name=upload_name,
                payload=payload,
                target_library=target_library,
                selected_symbol=selected_symbol,
                counterpart_asset_id=counterpart_asset_id,
                actor=actor,
            )
            if result["mode"] == "imported":
                conn.commit()
        if result["mode"] == "imported":
            result["component"] = self.get_component(component_id)
        return result

    def _extract_footprints_from_upload(self, upload_name: str, payload: bytes) -> dict[str, bytes]:
        return self._asset_imports.extract_footprints_from_upload(upload_name, payload)

    def import_footprint(
        self,
        component_id: str,
        *,
        upload_name: str,
        payload: bytes,
        target_library: str,
        selected_footprint: str,
        counterpart_asset_id: str = "",
        actor: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            result = self._asset_imports.import_footprint(
                conn,
                self._runtime_for_compat(),
                component_id,
                upload_name=upload_name,
                payload=payload,
                target_library=target_library,
                selected_footprint=selected_footprint,
                counterpart_asset_id=counterpart_asset_id,
                actor=actor,
            )
            if result["mode"] == "imported":
                conn.commit()
        if result["mode"] == "imported":
            result["component"] = self.get_component(component_id)
        return result

    def attach_auxiliary_asset(
        self,
        component_id: str,
        *,
        asset_type: str,
        upload_name: str,
        payload: bytes,
        target_library: str,
        actor: str = "",
    ) -> dict[str, Any]:
        if asset_type not in {"3dmodel", "spice"}:
            raise ValueError("Unsupported auxiliary asset type")
        self.initialize()
        with self._connect() as conn:
            self._asset_imports.attach_auxiliary_asset(
                conn,
                self._runtime_for_compat(),
                component_id,
                asset_type=asset_type,
                upload_name=upload_name,
                payload=payload,
                target_library=target_library,
                actor=actor,
            )
            conn.commit()
        return {"component": self.get_component(component_id)}

    def detach_asset(self, component_id: str, asset_type: str, *, actor: str = "") -> dict[str, Any]:
        if asset_type not in SUPPORTED_ASSET_TYPES:
            raise ValueError("Unsupported asset type")
        if asset_type in PLACE_REQUIRED_ASSET_TYPES:
            raise ValueError(
                "Symbol and footprint assets must be detached by asset ID after removing or reassigning their representations"
            )
        self.initialize()
        with self._connect() as conn:
            if self._asset_imports.detach_asset(
                conn, self._runtime_for_compat(), component_id, asset_type, actor=actor
            ):
                conn.commit()
        return {"component": self.get_component(component_id)}

    def detach_asset_by_id(
        self,
        component_id: str,
        asset_id: str,
        *,
        expected_revision_id: str,
        actor: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._asset_imports.detach_asset_by_id(
                conn,
                self._runtime_for_compat(),
                component_id,
                asset_id,
                expected_revision_id=expected_revision_id,
                actor=actor,
            )
            conn.commit()
        return {"component": self.get_component(component_id)}

    def _klc_release_gate(self) -> str:
        return self._klc_validation.release_gate()

    def _klc_utils_root(self) -> Path:
        return self._klc_validation.utils_root()

    def _klc_checker_path(self, asset_type: str) -> Path | None:
        return self._klc_validation.checker_path(asset_type)

    def _klc_tool_version(self) -> str:
        return self._klc_validation.tool_version()

    def _klc_rule_args(self, asset_type: str) -> list[str]:
        return self._klc_validation.rule_args(asset_type)

    def _parse_klc_junit(self, junit_path: Path) -> list[dict[str, Any]]:
        return self._klc_validation.parse_klc_junit(junit_path)

    def _write_validation_report_json(self, path: Path, **report: Any) -> None:
        self._klc_validation.write_validation_report_json(path, **report)

    def _store_validation_run(self, conn: Any, **run: Any) -> dict[str, Any]:
        return self._klc_validation.store_validation_run(conn, **run)

    def _run_klc_for_asset(
        self,
        conn: Any,
        *,
        component_id: str,
        revision_id: str,
        asset: dict[str, Any],
    ) -> dict[str, Any]:
        return self._klc_validation.run_klc_for_asset(
            conn,
            self._runtime_for_compat(),
            component_id=component_id,
            revision_id=revision_id,
            asset=asset,
        )

    def validate_component_klc(self, component_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            component, revision, runs = self._klc_validation.validate_component(
                conn, self._runtime_for_compat(), component_id
            )
            conn.commit()
            component_payload = self._component_payload(conn, component, revision)
        return {"component": component_payload, "runs": runs}

    def get_component_validation(self, component_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._klc_validation.component_validation(conn, component_id)

    def get_validation_run(self, run_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            return self._klc_validation.validation_run(conn, run_id)

    def validation_report_path(self, run_id: str, report_name: str) -> Path | None:
        self.initialize()
        with self._connect() as conn:
            return self._klc_validation.report_path(conn, self._runtime_for_compat(), run_id, report_name)

    def catalog_health(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            return self._catalog_health.report(conn)

    def regenerate_component_previews(self, component_id: str, *, actor: str = "") -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._regenerate_component_previews_in_conn(
                conn,
                component_id,
                actor=actor or "preview-generator",
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def set_release_status(
        self,
        component_id: str,
        release_status: str,
        *,
        actor: str = "",
        self_approval_override_reason: str = "",
        review_note: str = "",
        actor_role: str = "",
        expected_revision_id: str = "",
        expected_manifest_hash: str = "",
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            self._release_workflow.set_release_status(
                conn,
                self._runtime_for_compat(),
                component_id,
                release_status,
                actor=actor,
                self_approval_override_reason=self_approval_override_reason,
                review_note=review_note,
                actor_role=actor_role,
                expected_revision_id=expected_revision_id,
                expected_manifest_hash=expected_manifest_hash,
            )
            conn.commit()
        return self.get_component(component_id) or {}

    def deactivate_component(self, component_id: str, *, actor: str = "", reason: str = "") -> bool:
        self.initialize()
        with self._connect() as conn:
            deactivated = self._release_workflow.deactivate_component(
                conn, component_id, actor=actor, reason=reason
            )
            conn.commit()
            return deactivated

    def delete_component(self, component_id: str, *, actor: str = "", reason: str = "") -> bool:
        # Component identity, revisions, releases, usage, and audit evidence are never
        # hard-deleted. The legacy DELETE contract now performs an auditable tombstone
        # so existing callers retain their UX while compliance history remains intact.
        return self.deactivate_component(component_id, actor=actor, reason=reason)

    def _materialize_asset(self, asset: dict[str, Any], assets_for_revision: list[dict[str, Any]], component: dict[str, Any] | None = None) -> dict[str, Any]:
        path = Path(str(asset["canonical_path"]))
        payload = path.read_bytes()
        if asset["asset_type"] == "symbol":
            footprint_asset = next((candidate for candidate in assets_for_revision if candidate["asset_type"] == "footprint"), None)
            footprint_ref = None
            if footprint_asset:
                footprint_ref = f"{_remote_library_nickname(str(footprint_asset['target_library']))}:{footprint_asset['target_name']}"
            payload = _rewrite_symbol_payload(payload, footprint_ref, component)
        elif asset["asset_type"] == "footprint":
            payload = _rewrite_footprint_payload(
                payload,
                asset,
                [candidate for candidate in assets_for_revision if candidate["asset_type"] == "3dmodel"],
            )
        content_type = _content_type_for_asset(str(asset["asset_type"]), path)
        return {
            **asset,
            "payload": payload,
            "content_type": content_type,
            "size_bytes": len(payload),
            "sha256": _sha256_bytes(payload),
            "name": path.name,
        }

    def _placement_assets(
        self, conn: Any, revision_id: str, representation_id: str = ""
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if representation_id:
            row = conn.execute(
                "SELECT * FROM revision_representations WHERE id = %s AND revision_id = %s",
                (representation_id, revision_id),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM revision_representations WHERE revision_id = %s AND is_default = 1 LIMIT 1",
                (revision_id,),
            ).fetchone()
        if not row:
            raise ValueError("Representation was not found on this revision")
        if not row["symbol_asset_id"] or not row["footprint_asset_id"]:
            raise ValueError("Selected representation is incomplete")
        all_assets = self._load_assets_for_revision(conn, revision_id)
        selected_ids = {str(row["symbol_asset_id"]), str(row["footprint_asset_id"])}
        assets = [
            asset
            for asset in all_assets
            if str(asset["asset_type"]) in {"3dmodel", "spice"} or str(asset["id"]) in selected_ids
        ]
        if len([asset for asset in assets if str(asset["id"]) in selected_ids]) != 2:
            raise ValueError("Selected representation references unavailable assets")
        return dict(row), assets

    def build_manifest(
        self, component_id: str, base_url: str, representation_id: str = ""
    ) -> dict[str, Any] | None:
        self.initialize()
        component = self.get_component(component_id, include_inactive=False, released_only=True)
        if not component:
            return None
        if not component["place_enabled"]:
            raise ValueError("Component is not placeable because it is not released or required files are missing")
        with self._connect() as conn:
            representation, assets = self._placement_assets(
                conn, component["revision_id"], representation_id
            )
        manifest_assets = []
        for raw_asset in assets:
            asset = self._materialize_asset(raw_asset, assets, component)
            manifest_assets.append(
                {
                    "asset_type": asset["asset_type"],
                    "name": asset["name"],
                    "target_library": asset["target_library"],
                    "target_name": asset["target_name"],
                    "content_type": asset["content_type"],
                    "size_bytes": asset["size_bytes"],
                    "sha256": asset["sha256"],
                    "required": bool(raw_asset["required"]),
                    "download_url": self.build_signed_asset_url(
                        asset["id"], component["revision_id"], base_url,
                        representation_id=str(representation["id"]),
                    ),
                }
            )
        return {
            "part_id": component["id"],
            "display_name": component["name"],
            "summary": component["summary"] or component["description"],
            "license": "Managed in KiCAD Prism",
            "representation_id": str(representation["id"]),
            "library_name": next(str(a["target_library"]) for a in assets if a["asset_type"] == "symbol"),
            "symbol_name": next(str(a["target_name"]) for a in assets if a["asset_type"] == "symbol"),
            "assets": manifest_assets,
        }

    def build_inline_bundle(
        self, component_id: str, representation_id: str = ""
    ) -> dict[str, Any] | None:
        self.initialize()
        component = self.get_component(component_id, include_inactive=False, released_only=True)
        if not component:
            return None
        if not component["place_enabled"]:
            raise ValueError("Component is not placeable because it is not released or required files are missing")
        with self._connect() as conn:
            representation, assets = self._placement_assets(
                conn, component["revision_id"], representation_id
            )
        bundle_entries = []
        for raw_asset in assets:
            asset = self._materialize_asset(raw_asset, assets, component)
            bundle_entries.append(
                {
                    "type": asset["asset_type"],
                    "name": asset["name"] if asset["asset_type"] == "3dmodel" else asset["target_name"] or asset["name"],
                    "compression": "NONE",
                    "content": base64.b64encode(asset["payload"]).decode("ascii"),
                    "checksum": asset["sha256"],
                }
            )
        return {
            "part_id": component["id"],
            "display_name": component["name"],
            "representation_id": str(representation["id"]),
            "library": next(str(a["target_library"]) for a in assets if a["asset_type"] == "symbol"),
            "symbol_name": next(str(a["target_name"]) for a in assets if a["asset_type"] == "symbol"),
            "compression": "NONE",
            "data": base64.b64encode(json.dumps(bundle_entries, separators=(",", ":")).encode("utf-8")).decode("ascii"),
        }

    def get_asset_by_id(
        self, asset_id: str, *, revision_id: str = "", representation_id: str = ""
    ) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM assets WHERE id = %s", (asset_id,)).fetchone()
            if not row:
                return None
            asset = dict(row)
            effective_revision_id = revision_id
            if not effective_revision_id:
                link = conn.execute("SELECT revision_id FROM revision_assets WHERE asset_id = %s ORDER BY updated_at DESC LIMIT 1", (asset_id,)).fetchone()
                effective_revision_id = str(link["revision_id"]) if link else ""
            if effective_revision_id and representation_id:
                _, assets_for_revision = self._placement_assets(
                    conn, effective_revision_id, representation_id
                )
            else:
                assets_for_revision = self._load_assets_for_revision(conn, effective_revision_id) if effective_revision_id else [asset]
            component = None
            if effective_revision_id:
                revision = self._revision_row(conn, effective_revision_id)
                if revision:
                    component_row = self._component_row(conn, str(revision["component_id"]))
                    if component_row:
                        component = self._component_payload(conn, component_row, revision)
        return self._materialize_asset(asset, assets_for_revision, component)

    def get_preview(self, preview_id: str) -> CatalogPreview | None:
        self.initialize()
        with self._connect() as conn:
            return self._preview_pipeline.preview_record(conn, preview_id)

    def _sign(self, message: str) -> str:
        if not settings.SESSION_SECRET:
            raise RuntimeError("SESSION_SECRET is required to sign catalog asset URLs")
        secret = settings.SESSION_SECRET.encode("utf-8")
        return base64.urlsafe_b64encode(hmac.new(secret, message.encode("utf-8"), hashlib.sha256).digest()).rstrip(b"=").decode("ascii")

    def build_signed_asset_url(
        self, asset_id: str, revision_id: str, base_url: str, ttl_seconds: int = 300,
        *, representation_id: str = "",
    ) -> str:
        expires_at = int(time.time()) + ttl_seconds
        signature = self._sign(f"{asset_id}:{revision_id}:{representation_id}:{expires_at}")
        return (
            f"{base_url.rstrip('/')}/api/remote-provider/assets/{asset_id}?rev={revision_id}"
            f"&representation={representation_id}&exp={expires_at}&sig={signature}"
        )

    def validate_asset_signature(
        self, asset_id: str, revision_id: str, expires_at: int, signature: str,
        representation_id: str = "",
    ) -> bool:
        if expires_at <= int(time.time()):
            return False
        return hmac.compare_digest(
            self._sign(f"{asset_id}:{revision_id}:{representation_id}:{expires_at}"), signature
        )

    def store_auth_code(self, code: str, grant: dict[str, Any], exp: int) -> None:
        self.initialize()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO oauth_auth_codes (code, grant_json, exp)
                VALUES (%s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET grant_json = excluded.grant_json, exp = excluded.exp
                """,
                (code, json.dumps(grant, separators=(",", ":")), exp),
            )
            conn.commit()

    def consume_auth_code(self, code: str) -> dict[str, Any] | None:
        self.initialize()
        now = int(time.time())
        with self._connect() as conn:
            row = conn.execute("SELECT grant_json, exp FROM oauth_auth_codes WHERE code = %s", (code,)).fetchone()
            conn.execute("DELETE FROM oauth_auth_codes WHERE code = %s", (code,))
            conn.execute("DELETE FROM oauth_auth_codes WHERE exp <= %s", (now,))
            conn.commit()
        if not row or int(row["exp"]) <= now:
            return None
        return dict(_json_loads(row["grant_json"], {}))

    def add_revoked_token(self, jti: str, exp: int) -> None:
        self.initialize()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO oauth_revoked_tokens (jti, exp)
                VALUES (%s, %s)
                ON CONFLICT (jti) DO UPDATE SET exp = excluded.exp
                """,
                (jti, exp),
            )
            conn.commit()

    def is_token_revoked(self, jti: str) -> bool:
        self.initialize()
        now = int(time.time())
        with self._connect() as conn:
            conn.execute("DELETE FROM oauth_revoked_tokens WHERE exp <= %s", (now,))
            row = conn.execute("SELECT 1 FROM oauth_revoked_tokens WHERE jti = %s", (jti,)).fetchone()
            conn.commit()
        return bool(row)

    def _released_place_ready_components(self) -> list[dict[str, Any]]:
        return [
            component
            for component in self.list_components_flat(released_only=True, include_inactive=False)
            if component["place_enabled"]
        ]

    def _dbl_row_for_component(
        self,
        component: dict[str, Any],
        part_number: str,
        custom_fields: list[dict[str, Any]],
    ) -> dict[str, str]:
        default_representation = next(
            (item for item in component.get("representations", []) if item.get("is_default")),
            None,
        )
        symbol_asset = default_representation.get("symbol") if default_representation else None
        footprint_asset = default_representation.get("footprint") if default_representation else None
        lib_symbol = ""
        lib_footprint = ""
        if symbol_asset:
            lib_symbol = f"{_dbl_symbol_library_name(part_number, symbol_asset)}:{symbol_asset['target_name']}"
        if footprint_asset:
            lib_footprint = f"{footprint_asset['target_library']}:{footprint_asset['target_name']}"
        row = {
            "Part Number": part_number,
            "Part Number Nocolon": part_number,
            "Comment": component["value"] or component["name"],
            "Value": component["value"],
            "Manufacturer": component["manufacturer"],
            "Manufacturer Part Number": component["mpn"],
            "PackageDescription": component["package_name"],
            "Status": component["workflow_stage"],
            "Part Description": component["description"],
            "Datasheet": component["datasheet_url"],
            "LibSymbol": lib_symbol,
            "LibFootprint": lib_footprint,
        }
        extras = dict(component.get("extra_fields") or {})
        row.update({field["key"]: str(extras.get(field["storage_key"], "")) for field in custom_fields})
        return row

    def _collect_dbl_assets(
        self,
        component: dict[str, Any],
        part_number: str,
        export_root: Path,
        conn: Any,
    ) -> None:
        _, assets = self._placement_assets(conn, component["revision_id"])
        for raw_asset in assets:
            if raw_asset["asset_type"] not in {"symbol", "footprint"}:
                continue
            asset = self._materialize_asset(raw_asset, assets, component)
            if raw_asset["asset_type"] == "symbol":
                library_name = _dbl_symbol_library_name(part_number, asset)
                destination = export_root / "SchLib" / f"{library_name}.kicad_sym"
            else:
                destination = export_root / "PcbLib" / f"{asset['target_library']}.pretty" / f"{asset['target_name']}.kicad_mod"
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(asset["payload"])

    def _write_dbl_config(self, export_root: Path, *, filename: str, connection_string: str, libraries: list[dict[str, Any]]) -> None:
        payload = {
            "meta": {"version": 0},
            "name": "KiCAD Prism Database Library",
            "description": "KiCAD Prism released component database library",
            "source": {
                "type": "odbc",
                "dsn": "",
                "username": "",
                "password": "",
                "timeout_seconds": 2,
                "connection_string": connection_string,
            },
            "cache": {"max_age": 28800},
            "libraries": libraries,
        }
        (export_root / filename).write_text(json.dumps(payload, indent=4) + "\n", encoding="utf-8")

    def export_kicad_dbl_bundle(self) -> dict[str, Any]:
        # The generated KiCad database-library bundle intentionally uses SQLite as
        # an interchange artifact. Prism's runtime state is PostgreSQL-only.
        import sqlite3 as sqlite_export

        self.initialize()
        export_root = self.export_root
        if export_root.exists():
            shutil.rmtree(export_root)
        (export_root / "SchLib").mkdir(parents=True, exist_ok=True)
        (export_root / "PcbLib").mkdir(parents=True, exist_ok=True)

        components = sorted(self._released_place_ready_components(), key=lambda c: (c["category"], c["mpn"], c["id"]))
        custom_fields = [
            field for field in self.list_metadata_fields()
            if field["storage_kind"] == "extra" and field["key"] not in DBL_COMMON_COLUMNS
        ]
        custom_columns = [field["key"] for field in custom_fields]
        effective_columns = (*DBL_COMMON_COLUMNS, *custom_columns)
        db_path = export_root / "Prism.sqlite"
        used_part_numbers: set[str] = set()
        grouped_rows: dict[str, list[dict[str, str]]] = {}

        with self._connect() as catalog_conn:
            for component in components:
                base_part = _part_number_nocolon(component["mpn"] or component["value"] or component["id"])
                part_number = base_part
                counter = 2
                while part_number in used_part_numbers:
                    part_number = f"{base_part}_{counter}"
                    counter += 1
                used_part_numbers.add(part_number)
                category = component["category"] or "Uncategorized"
                grouped_rows.setdefault(category, []).append(self._dbl_row_for_component(component, part_number, custom_fields))
                self._collect_dbl_assets(component, part_number, export_root, catalog_conn)

        with sqlite_export.connect(db_path) as dbl_conn:
            for category, rows in sorted(grouped_rows.items()):
                table = _quote_identifier(category)
                columns_sql = ", ".join(f"{_quote_identifier(column)} TEXT NOT NULL DEFAULT ''" for column in effective_columns)
                dbl_conn.execute(f"CREATE TABLE {table} ({columns_sql})")
                column_names = ", ".join(_quote_identifier(column) for column in effective_columns)
                placeholders = ", ".join("?" for _ in effective_columns)
                for row in rows:
                    dbl_conn.execute(
                        f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})",
                        tuple(row.get(column, "") for column in effective_columns),
                    )

        fields = [
            {
                "column": column,
                "name": column,
                "visible_on_add": False,
                "visible_in_chooser": column not in {"LibSymbol", "LibFootprint"},
                "show_name": True,
                "inherit_properties": True,
            }
            for column in effective_columns
            if column not in {"Part Number Nocolon"}
        ]
        libraries = [
            {
                "name": category,
                "table": category,
                "key": "Part Number Nocolon",
                "symbols": "LibSymbol",
                "footprints": "LibFootprint",
                "fields": fields,
            }
            for category in sorted(grouped_rows)
        ]
        self._write_dbl_config(
            export_root,
            filename="Prism_Linux.kicad_dbl",
            connection_string="Driver={SQLite3};Database=${CWD}/Prism.sqlite;",
            libraries=libraries,
        )
        self._write_dbl_config(
            export_root,
            filename="Prism_Windows.kicad_dbl",
            connection_string="Driver={SQLite3 ODBC Driver};Database=${CWD}/Prism.sqlite;",
            libraries=libraries,
        )

        symbol_libraries = sorted(path.stem for path in (export_root / "SchLib").glob("*.kicad_sym"))
        footprint_libraries = sorted({asset["target_library"] for component in components for asset in component["assets"] if asset["asset_type"] == "footprint"})
        sym_lines = [
            '(sym_lib_table',
            '  (lib (name "Prism")(type "Database")(uri "${PRISM_LIB_DIR}/Prism_Linux.kicad_dbl")(options "")(descr ""))',
        ]
        sym_lines.extend(
            f'  (lib (name "{_sexpr_string(library)}")(type "KiCad")(uri "${{PRISM_LIB_DIR}}/SchLib/{_sexpr_string(library)}.kicad_sym")(options "")(descr "")(hidden))'
            for library in symbol_libraries
        )
        sym_lines.append(")")
        (export_root / "sym-lib-table").write_text("\n".join(sym_lines) + "\n", encoding="utf-8")

        fp_lines = ["(fp_lib_table"]
        fp_lines.extend(
            f'  (lib (name "{_sexpr_string(library)}")(type "KiCad")(uri "${{PRISM_LIB_DIR}}/PcbLib/{_sexpr_string(library)}.pretty")(options "")(descr ""))'
            for library in footprint_libraries
        )
        fp_lines.append(")")
        (export_root / "fp-lib-table").write_text("\n".join(fp_lines) + "\n", encoding="utf-8")

        return {
            "export_root": str(export_root),
            "component_count": len(components),
            "category_count": len(grouped_rows),
            "sqlite_path": str(db_path),
            "linux_dbl": str(export_root / "Prism_Linux.kicad_dbl"),
            "windows_dbl": str(export_root / "Prism_Windows.kicad_dbl"),
            "sym_lib_table": str(export_root / "sym-lib-table"),
            "fp_lib_table": str(export_root / "fp-lib-table"),
        }
