"""Source-aware design-variant catalog discovery (VAR-07).

The catalog is the union of four source sets, in the order frozen in the
design-variant contract packet v1.0 section 2.1:

1. the ``.kicad_pro`` registry (``schematic.variants``), in file order;
2. board-header names from ``(variants …)``, in file order;
3. names in schematic symbol- and sheet-instance variant records, in hierarchy
   walk order then file order;
4. names in footprint variant records, in board file order.

Board-side names (2 and 4) fold case-insensitively into the first matching
catalog entry with a ``catalog-name-case-mismatch`` diagnostic; schematic names
stay distinct (KiCad's schematic variant names are case-sensitive). An
unreadable or malformed source is skipped with a ``source-unparseable``
diagnostic: the readable sources still produce a catalog.

Discovery reads text with the semantic index's balanced S-expression scanner,
so quoted strings and escaped quotes never confuse the form walk, and it reads
the revision through ``project_source_snapshot`` so a commit never leaks
working-tree names. No kicad-monkey call and no private ``projects.py`` helper
is involved; the API layer supplies a role-validated project.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from app.services import semantic_index_service
from app.services.project_source_snapshot import (
    ProjectSourceSnapshot,
    project_revision_identity,
    project_source_snapshot,
    source_files,
)

SCHEMA = "prism.project_variants_a0"

SOURCE_PROJECT = "project"
SOURCE_PCB = "pcb"
SOURCE_SCHEMATIC = "schematic"
SOURCE_FOOTPRINT = "footprint"

CASE_MISMATCH_CODE = "catalog-name-case-mismatch"
SOURCE_UNPARSEABLE_CODE = "source-unparseable"

_VARIANT_START = re.compile(r"\(variant(?=\s|\))")
_VARIANTS_START = re.compile(r"\(variants(?=\s|\))")
_NAME_VALUE = re.compile(r'\(name\s+"((?:\\.|[^"\\])*)"')
_DESCRIPTION_VALUE = re.compile(r'\(description\s+"((?:\\.|[^"\\])*)"')
_SHEETFILE_VALUE = re.compile(r'\(property\s+"Sheetfile"\s+"((?:\\.|[^"\\])*)"')


def _unescape(value: str) -> str:
    return value.replace(r"\"", '"').replace(r"\\", "\\")


def _balanced_file(text: str) -> bool:
    """True when the whole file is one or more complete forms and nothing else.

    A malformed file can still contain a balanced ``(variant …)`` record; the
    catalog only trusts records from files that parse as a whole, which is why
    the malformed fixture contributes no schematic names.
    """

    start = text.find("(")
    if start < 0:
        return False
    end = semantic_index_service._balanced_s_expression_end(text, start)
    if end is None:
        return False
    return not text[end:].strip()


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None


class _CatalogBuilder:
    """Ordered catalog plus diagnostics, with the packet's fold rules."""

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []
        self._diagnostics: list[dict[str, Any]] = []
        self._case_folds: set[tuple[str, str]] = set()

    def add_project_name(self, name: str, description: Optional[str]) -> None:
        exact = self._exact(name)
        if exact is not None:
            _add_source(exact, SOURCE_PROJECT)
            _merge_description(exact, description)
            return
        self._entries.append(
            {
                "name": name,
                "description": description,
                "sources": [SOURCE_PROJECT],
            }
        )

    def add_board_name(
        self,
        name: str,
        description: Optional[str],
        source: str,
        kind: str,
    ) -> None:
        # Board-side names always resolve against the first catalog entry with
        # that spelling, even when a later entry matches exactly: the native
        # board map is a single case-insensitive key space (case_fold fixture).
        folded = self._folded(name)
        if folded is not None:
            _add_source(folded, source)
            _merge_description(folded, description)
            if str(folded["name"]) != name:
                self._case_mismatch(kind, name, str(folded["name"]))
            return
        self._entries.append(
            {"name": name, "description": description, "sources": [source]}
        )

    def add_schematic_name(self, name: str) -> None:
        exact = self._exact(name)
        if exact is not None:
            _add_source(exact, SOURCE_SCHEMATIC)
            return
        self._entries.append(
            {"name": name, "description": None, "sources": [SOURCE_SCHEMATIC]}
        )

    def source_unparseable(self, source: str, path: str) -> None:
        self._diagnostics.append(
            {
                "code": SOURCE_UNPARSEABLE_CODE,
                "severity": "error",
                "source": source,
                "path": path,
                "message": f"Could not read {path} as a KiCad source.",
            }
        )

    def _case_mismatch(self, kind: str, found: str, folded_into: str) -> None:
        key = (found, folded_into)
        if key in self._case_folds:
            return
        self._case_folds.add(key)
        self._diagnostics.append(
            {
                "code": CASE_MISMATCH_CODE,
                "severity": "warning",
                "source": SOURCE_PCB,
                "message": f'{kind} name "{found}" folded into "{folded_into}".',
                "detail": {"found": found, "folded_into": folded_into},
            }
        )

    def _exact(self, name: str) -> Optional[dict[str, Any]]:
        return next((entry for entry in self._entries if entry["name"] == name), None)

    def _folded(self, name: str) -> Optional[dict[str, Any]]:
        folded = name.lower()
        return next(
            (
                entry
                for entry in self._entries
                if str(entry["name"]).lower() == folded
            ),
            None,
        )

    def variants(self) -> list[dict[str, Any]]:
        return [
            {
                "name": entry["name"],
                "description": entry["description"],
                "sources": entry["sources"],
            }
            for entry in self._entries
        ]

    def diagnostics(self) -> list[dict[str, Any]]:
        return list(self._diagnostics)


def _add_source(entry: dict[str, Any], source: str) -> None:
    sources: list[str] = entry["sources"]
    if source not in sources:
        sources.append(source)


def _merge_description(entry: dict[str, Any], description: Optional[str]) -> None:
    if entry["description"] is None and description is not None:
        entry["description"] = description


def _project_registry_entries(project_file: Path) -> tuple[list[dict[str, Any]], bool]:
    """``(entries, readable)`` for ``$.schematic.variants`` (packet N10)."""

    try:
        raw = json.loads(project_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return [], False
    if not isinstance(raw, dict):
        return [], False
    schematic = raw.get("schematic")
    if schematic is None:
        return [], True
    if not isinstance(schematic, dict):
        return [], False
    variants = schematic.get("variants")
    if variants is None:
        return [], True
    if not isinstance(variants, list):
        return [], False

    entries: list[dict[str, Any]] = []
    for item in variants:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name:
            continue
        description = item.get("description")
        entries.append(
            {
                "name": name,
                "description": description if isinstance(description, str) else None,
            }
        )
    return entries, True


def _board_records(
    text: str,
) -> tuple[list[tuple[str, Optional[str]]], list[str], bool]:
    """``(header entries, footprint record names, readable)`` for a board."""

    if not _balanced_file(text):
        return [], [], False

    header_span: Optional[tuple[int, int]] = None
    header_entries: list[tuple[str, Optional[str]]] = []
    header = _VARIANTS_START.search(text)
    if header is not None:
        end = semantic_index_service._balanced_s_expression_end(text, header.start())
        if end is None:
            return [], [], False
        header_span = (header.start(), end)
        block = text[header.start() : end]
        for match in _VARIANT_START.finditer(block):
            entry_end = semantic_index_service._balanced_s_expression_end(
                block, match.start()
            )
            if entry_end is None:
                return [], [], False
            entry = block[match.start() : entry_end]
            name = _NAME_VALUE.search(entry)
            if name is None:
                continue
            description = _DESCRIPTION_VALUE.search(entry)
            header_entries.append(
                (
                    _unescape(name.group(1)),
                    _unescape(description.group(1)) if description else None,
                )
            )

    footprint_names: list[str] = []
    for match in _VARIANT_START.finditer(text):
        if header_span is not None and header_span[0] <= match.start() < header_span[1]:
            continue
        end = semantic_index_service._balanced_s_expression_end(text, match.start())
        if end is None:
            return [], [], False
        name = _NAME_VALUE.search(text, match.start(), end)
        if name is not None:
            footprint_names.append(_unescape(name.group(1)))
    return header_entries, footprint_names, True


def _schematic_record_names(text: str) -> tuple[list[str], bool]:
    """Names from symbol- and sheet-instance variant records, in file order."""

    if not _balanced_file(text):
        return [], False
    names: list[str] = []
    for match in _VARIANT_START.finditer(text):
        end = semantic_index_service._balanced_s_expression_end(text, match.start())
        if end is None:
            return [], False
        name = _NAME_VALUE.search(text, match.start(), end)
        if name is not None:
            names.append(_unescape(name.group(1)))
    return names, True


def _schematic_hierarchy(root_schematic: Path) -> list[Path]:
    """The root sheet then every sheet it instantiates, depth-first.

    ``Sheetfile`` references are followed in file order and resolved relative
    to the sheet that owns them, which is how KiCad writes hierarchical
    paths. A repeated sheet is visited once; its names are already counted.
    """

    ordered: list[Path] = []
    visited: set[Path] = set()

    def visit(path: Path) -> None:
        resolved = path.resolve()
        if resolved in visited:
            return
        visited.add(resolved)
        ordered.append(resolved)
        text = _read_text(resolved)
        if text is None:
            return
        for reference in _SHEETFILE_VALUE.findall(text):
            visit(resolved.parent / _unescape(reference))

    visit(root_schematic)
    return ordered


def _relative(snapshot: ProjectSourceSnapshot, path: Path) -> str:
    try:
        return path.resolve().relative_to(snapshot.root).as_posix()
    except ValueError:
        return path.name


def discover_variant_catalog(
    project: Any, commit: Optional[str] = None
) -> dict[str, Any]:
    """Discover the variant catalog for one project revision.

    ``project`` must already have passed the caller's role check; this service
    performs no authorization. ``commit`` is any ref accepted by the semantic
    index (resolved to a SHA here), or ``None`` for the working tree.
    """

    source_revision_key, resolved_commit = project_revision_identity(project, commit)
    builder = _CatalogBuilder()

    with project_source_snapshot(project, commit) as snapshot:
        board, schematic = source_files(project, snapshot)

        if snapshot.project_file.suffix.lower() == ".kicad_pro":
            registry_entries, registry_readable = _project_registry_entries(
                snapshot.project_file
            )
            if not registry_readable:
                builder.source_unparseable(
                    SOURCE_PROJECT, _relative(snapshot, snapshot.project_file)
                )
            for entry in registry_entries:
                builder.add_project_name(entry["name"], entry["description"])

        header_entries: list[tuple[str, Optional[str]]] = []
        footprint_names: list[str] = []
        if board is not None:
            board_text = _read_text(board)
            if board_text is None:
                builder.source_unparseable(SOURCE_PCB, _relative(snapshot, board))
            else:
                header_entries, footprint_names, board_readable = _board_records(
                    board_text
                )
                if not board_readable:
                    builder.source_unparseable(
                        SOURCE_PCB, _relative(snapshot, board)
                    )

        for name, description in header_entries:
            builder.add_board_name(name, description, SOURCE_PCB, "Board header")

        if schematic is not None:
            for path in _schematic_hierarchy(schematic):
                text = _read_text(path)
                if text is None:
                    builder.source_unparseable(
                        SOURCE_SCHEMATIC, _relative(snapshot, path)
                    )
                    continue
                names, readable = _schematic_record_names(text)
                if not readable:
                    builder.source_unparseable(
                        SOURCE_SCHEMATIC, _relative(snapshot, path)
                    )
                    continue
                for name in names:
                    builder.add_schematic_name(name)

        for name in footprint_names:
            builder.add_board_name(name, None, SOURCE_FOOTPRINT, "Footprint record")

    return {
        "schema": SCHEMA,
        "projectId": str(getattr(project, "id", "")),
        "commit": resolved_commit,
        "sourceRevisionKey": source_revision_key,
        "variants": builder.variants(),
        "diagnostics": builder.diagnostics(),
    }
