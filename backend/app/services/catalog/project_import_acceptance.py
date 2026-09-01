"""Project-import acceptance preparation and proposal state transitions."""

from __future__ import annotations

from typing import Any


class CatalogProjectImportAcceptance:
    """Prepare import payloads and mutate proposal state on caller-owned connections."""

    @staticmethod
    def build_normalized_input(
        proposal: dict[str, Any],
        metadata_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        source_metadata = dict(proposal["metadata"])
        fields = dict(source_metadata.get("fields") or {})
        return {
            "value": source_metadata.get("value"),
            "description": source_metadata.get("description"),
            "datasheet": source_metadata.get("datasheet"),
            "manufacturer": source_metadata.get("manufacturer"),
            "manufacturer_part_number": source_metadata.get("manufacturer_part_number"),
            "package_name": source_metadata.get("footprint"),
            "vendor": fields.get("Vendor", ""),
            "vendor_part_number": fields.get("Vendor Part Number", ""),
            "mass_g": fields.get("Mass (g)", ""),
            "rqjc_c_w": fields.get("RQjC (C/W)", ""),
            "rqjc_top_c_w": fields.get("RQjC_top (C/W)", ""),
            "temp_max_c": fields.get("Temp_max (C)", ""),
            "temp_min_c": fields.get("Temp_min (C)", ""),
            "power_dissipation_w": fields.get("Power Dissipation (W)", ""),
            "rate": fields.get("Rate", ""),
            "extra_fields": fields,
            **(metadata_overrides or {}),
        }

    @staticmethod
    def claim_proposal(conn: Any, proposal_id: str, *, now: str) -> None:
        claimed = conn.execute(
            """
            UPDATE project_component_import_proposals
            SET status = 'accepting', updated_at = %s
            WHERE id = %s AND status = 'candidate'
            """,
            (now, proposal_id),
        )
        if claimed.rowcount == 0:
            raise ValueError("Project import proposal has already been resolved")

    @staticmethod
    def find_existing_component(conn: Any, manufacturer: str, mpn: str) -> Any | None:
        return conn.execute(
            """
            SELECT c.id
            FROM components c
            JOIN component_revisions revision ON revision.id = c.current_revision_id
            WHERE c.is_active = 1 AND lower(revision.manufacturer) = lower(%s) AND lower(revision.mpn) = lower(%s)
            ORDER BY c.created_at
            LIMIT 1
            """,
            (manufacturer, mpn),
        ).fetchone()

    @staticmethod
    def build_import_payload(proposal: dict[str, Any]) -> dict[str, Any]:
        provenance = list(proposal["provenance"])
        provenance_source = str(provenance[0].get("source") or "project") if provenance else "project"
        import_source = "folder_snapshot" if provenance_source == "folder_snapshot" else "project"
        external_id = (
            str(provenance[0].get("snapshotId") or provenance[0].get("projectId") or "")
            if provenance
            else ""
        )
        return {
            "provenance": provenance,
            "import_source": import_source,
            "external_id": external_id,
        }

    @staticmethod
    def mark_proposal_accepted(
        conn: Any,
        proposal_id: str,
        component_id: str,
        *,
        now: str,
    ) -> None:
        conn.execute(
            """
            UPDATE project_component_import_proposals
            SET status = 'accepted', accepted_component_id = %s, updated_at = %s
            WHERE id = %s AND status = 'accepting'
            """,
            (component_id, now, proposal_id),
        )


__all__ = ["CatalogProjectImportAcceptance"]
