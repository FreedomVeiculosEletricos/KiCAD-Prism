"""Read a project's KiCad sources at one revision (VAR-07).

The variant catalog reads source text without kicad-monkey, but it must read
the same revision the semantic index would: the configured anchor picks the
project file, the working tree is read in place, and a commit is extracted to
a temporary checkout that lives for the duration of the context.

This is a narrow public seam over machinery the semantic index already owns.
It deliberately does not reimplement git or project-file discovery:
``semantic_visualizer_service`` resolves refs and extracts trees, and
``semantic_index_service`` owns the cache-identity rules. A project whose
anchor is a board or schematic instead of a ``.kicad_pro`` (PCB-only projects)
is supported by keying the same source tree to that anchor.
"""

from __future__ import annotations

import contextlib
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Optional

from app.services import (
    path_config_service,
    semantic_index_service,
    semantic_visualizer_service,
)

PROJECT_SUFFIXES = (".kicad_pro", ".kicad_pcb", ".kicad_sch")


@dataclass(frozen=True)
class ProjectSourceSnapshot:
    """One revision of a project's source tree, already materialized."""

    root: Path
    project_file: Path
    commit: Optional[str]


def _anchor_source(project: Any) -> Optional[Path]:
    """The anchor itself when it is a board or schematic rather than a project.

    PCB-only projects have no ``.kicad_pro``; the anchor that selected them is
    the board (or a lone schematic). ``find_kicad_project`` would raise for
    them, so the snapshot keys the same tree to the anchor that exists.
    """

    anchor = path_config_service.anchor_for_project(project)
    if not anchor or anchor.endswith(".kicad_pro"):
        return None
    if not anchor.endswith(PROJECT_SUFFIXES):
        return None
    candidate = (Path(project.path) / anchor).resolve()
    if not candidate.is_file():
        raise ValueError(f"Configured source not found at the anchor: {anchor}")
    return candidate


def project_revision_identity(
    project: Any, commit: Optional[str] = None
) -> tuple[str, Optional[str]]:
    """The index's own ``(sourceRevisionKey, resolved commit)`` for a revision.

    Wrapping ``semantic_index_service._revision_identity`` keeps the catalog and
    the semantic index agreeing on what "this revision" means; the two must
    cache under the same key or a selection would pair mismatched data. For a
    board/schematic anchor the same primitives are applied with that anchor,
    which is the identity rule the index uses for ``project_file_rel``.
    """

    anchor = path_config_service.anchor_for_project(project) or ""
    source = _anchor_source(project)
    if source is None:
        return semantic_index_service._revision_identity(project, commit)

    if not commit:
        entries = semantic_index_service._source_entries_on_disk(source.parent)
        key = semantic_index_service._revision_key(entries, project_file_rel=anchor)
        return key, None

    repo_root = semantic_visualizer_service._repo_root(Path(project.path))
    resolved_commit = semantic_visualizer_service._resolve_commit(repo_root, commit)
    try:
        source_rel = source.relative_to(repo_root).as_posix()
    except ValueError as error:
        raise ValueError(
            f"Configured source is not inside the repository: {anchor}"
        ) from error
    project_dir = PurePosixPath(source_rel).parent.as_posix()
    if project_dir == ".":
        project_dir = ""
    entries = semantic_index_service._source_entries_in_commit(
        repo_root, resolved_commit, project_dir
    )
    key = semantic_index_service._revision_key(entries, project_file_rel=anchor)
    return key, resolved_commit


@contextlib.contextmanager
def project_source_snapshot(
    project: Any, commit: Optional[str] = None
) -> Iterator[ProjectSourceSnapshot]:
    """Yield the project's source root for a working tree or an exact commit.

    The anchor is the project's own ``.kicad_pro``/board filename recorded on
    the project; ``find_kicad_project`` (not the first file in the directory)
    resolves it. Commit checkouts are private to the context and removed when
    it exits.
    """

    anchor = path_config_service.anchor_for_project(project)
    anchor_source = _anchor_source(project)

    if not commit:
        if anchor_source is not None:
            yield ProjectSourceSnapshot(
                root=anchor_source.parent,
                project_file=anchor_source,
                commit=None,
            )
            return
        project_file = semantic_visualizer_service.find_kicad_project(
            project.path, anchor
        )
        resolved = project_file.resolve()
        yield ProjectSourceSnapshot(
            root=resolved.parent, project_file=resolved, commit=None
        )
        return

    repo_root = semantic_visualizer_service._repo_root(Path(project.path))
    resolved_commit = semantic_visualizer_service._resolve_commit(repo_root, commit)
    if anchor_source is not None:
        try:
            project_rel = anchor_source.relative_to(repo_root).as_posix()
        except ValueError as error:
            raise ValueError(
                f"Configured source is not inside the repository: {anchor}"
            ) from error
    else:
        project_rel = semantic_visualizer_service._project_relative_path(
            repo_root, Path(project.path), anchor
        )

    with tempfile.TemporaryDirectory(prefix="variant-catalog-commit-") as tmp:
        checkout = Path(tmp) / "checkout"
        semantic_visualizer_service._archive_checkout(
            repo_root, resolved_commit, checkout
        )
        project_file = checkout / project_rel
        if not project_file.is_file():
            raise ValueError(
                f"KiCad project file not found in commit {resolved_commit}: {project_rel}"
            )
        resolved = project_file.resolve()
        yield ProjectSourceSnapshot(
            root=resolved.parent, project_file=resolved, commit=resolved_commit
        )


def source_files(
    project: Any, snapshot: ProjectSourceSnapshot
) -> tuple[Optional[Path], Optional[Path]]:
    """The configured board and root schematic inside a snapshot.

    Selection mirrors ``path_config_service.resolve_paths``: an explicit path
    is used as named, a glob prefers the file whose stem matches the configured
    anchor, and the sorted first match is the fallback. Resolving against the
    snapshot root rather than the working tree is what keeps a commit read from
    leaking working-tree files.
    """

    anchor = path_config_service.anchor_for_project(project)
    config = path_config_service.get_path_config(str(project.path), anchor=anchor)
    stem = path_config_service.anchor_stem(anchor)

    def resolve(pattern: Optional[str], default: str) -> Optional[Path]:
        value = pattern or default
        if "*" not in value:
            candidate = snapshot.root / value
            return candidate if candidate.is_file() else None
        matches = sorted(
            snapshot.root.glob(value), key=lambda item: item.name.casefold()
        )
        if not matches:
            return None
        if stem:
            anchored = next(
                (
                    item
                    for item in matches
                    if item.stem.casefold() == stem.casefold()
                ),
                None,
            )
            if anchored is not None:
                return anchored
        return matches[0]

    board = resolve(config.pcb, "*.kicad_pcb")
    schematic = resolve(config.schematic, "*.kicad_sch")
    return board, schematic
