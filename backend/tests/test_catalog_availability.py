"""CAD availability is the default representation pair, not any attached assets."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.catalog.component_queries import CatalogComponentQueries  # noqa: E402
from app.services.catalog.component_read_models import (  # noqa: E402
    CatalogComponentReadModels,
    cad_availability,
    remote_place_enabled,
)
from app.services.catalog.metadata_normalization import (  # noqa: E402
    IDENTITY_KIND_MPN,
    IDENTITY_KIND_PROVISIONAL_IPN,
)


class CadAvailabilityContractTests(unittest.TestCase):
    def test_pair_completeness_matches_named_states(self) -> None:
        self.assertEqual(cad_availability(False, False), ("metadata_only", ["symbol", "footprint"]))
        self.assertEqual(cad_availability(True, False), ("files_partial", ["footprint"]))
        self.assertEqual(cad_availability(False, True), ("files_partial", ["symbol"]))
        self.assertEqual(cad_availability(True, True), ("place_ready", []))

    def test_place_enabled_stays_distinct_from_cad_completeness(self) -> None:
        complete: list[str] = []
        self.assertTrue(
            remote_place_enabled(
                is_active=True,
                identity_kind=IDENTITY_KIND_MPN,
                missing_assets=complete,
                release_status="released",
            )
        )
        self.assertFalse(
            remote_place_enabled(
                is_active=True,
                identity_kind=IDENTITY_KIND_MPN,
                missing_assets=complete,
                release_status="in_progress",
            )
        )
        self.assertFalse(
            remote_place_enabled(
                is_active=False,
                identity_kind=IDENTITY_KIND_MPN,
                missing_assets=complete,
                release_status="released",
            )
        )
        self.assertFalse(
            remote_place_enabled(
                is_active=True,
                identity_kind=IDENTITY_KIND_PROVISIONAL_IPN,
                missing_assets=complete,
                release_status="released",
            )
        )
        self.assertFalse(
            remote_place_enabled(
                is_active=True,
                identity_kind=IDENTITY_KIND_MPN,
                missing_assets=["footprint"],
                release_status="released",
            )
        )


class CatalogAvailabilityQueryTests(unittest.TestCase):
    def test_filters_and_sort_use_the_default_representation(self) -> None:
        queries = CatalogComponentQueries(component_read_models=object())  # type: ignore[arg-type]
        for state in ("place_ready", "files_partial", "metadata_only"):
            plan = queries.prepare_list_components(availability_state=state)
            self.assertIn("revision_representations", plan.where_sql)
            self.assertIn("is_default = 1", plan.where_sql)
            self.assertNotIn("revision_assets", plan.where_sql)
        sort_plan = queries.prepare_list_components(sort_by="availability_state")
        self.assertIn("revision_representations", sort_plan.order_sql)
        self.assertIn("is_default = 1", sort_plan.order_sql)
        self.assertNotIn("revision_assets", sort_plan.order_sql)


class CatalogAvailabilitySummaryTests(unittest.TestCase):
    def test_summary_follows_an_incomplete_default_despite_attached_assets(self) -> None:
        models = CatalogComponentReadModels(revision_kernel=object())  # type: ignore[arg-type]
        component = {
            "id": "c1",
            "slug": "c1",
            "source": "manual",
            "identity_kind": IDENTITY_KIND_MPN,
            "is_active": 1,
            "updated_at": "t",
        }
        revision = {
            "id": "r1",
            "name": "R",
            "value": "",
            "manufacturer": "M",
            "mpn": "P",
            "description": "",
            "package_name": "",
            "category": "",
            "datasheet_url": "",
            "vendor": "",
            "vendor_part_number": "",
            "mass_g": "",
            "rqjc_c_w": "",
            "rqjc_top_c_w": "",
            "temp_max_c": "",
            "temp_min_c": "",
            "power_dissipation_w": "",
            "rate": "",
            "sap_code": "",
            "extra_fields": "{}",
            "summary": "",
            "version": 1,
            "release_status": "draft",
            "updated_at": "t",
            "created_by": "",
        }
        assets = [
            {
                "id": "sym-default",
                "asset_type": "symbol",
                "target_library": "Def",
                "target_name": "S1",
            },
            {
                "id": "sym-other",
                "asset_type": "symbol",
                "target_library": "Other",
                "target_name": "S2",
            },
            {
                "id": "fp-other",
                "asset_type": "footprint",
                "target_library": "Other",
                "target_name": "F2",
            },
        ]
        payload = models.component_summary_payload(
            component,
            revision,
            assets,
            default_symbol_asset_id="sym-default",
            default_footprint_asset_id="",
        )
        self.assertEqual(payload["availability_state"], "files_partial")
        self.assertEqual(payload["missing_assets"], ["footprint"])
        self.assertFalse(payload["place_enabled"])
        self.assertEqual(payload["library_name"], "Def")
        self.assertEqual(payload["symbol_name"], "S1")


if __name__ == "__main__":
    unittest.main()
