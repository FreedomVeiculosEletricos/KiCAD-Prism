/**
 * VAR-14: resolution of `?variant=` against the fetched catalog and the
 * index's own catalog. Cases E16 (unknown name), E17 (identity mismatch) and
 * the empty/late/failed catalog states from the acceptance list.
 */
import { describe, expect, it } from "vitest";

import {
    requestedVariantFromSearchParams,
    resolveVariantSelection,
    variantSearchParams,
    variantSelectionNotice,
    variantSelectorDisabled,
    type VariantCatalogSnapshot,
} from "./variant-selection";
import type {
    AssemblyCatalogEntry,
    PrismSemanticIndex,
    SemanticComponent,
} from "@/types/prism-selection";

function component(reference: string): SemanticComponent {
    return {
        componentUid: `cmp:${reference}`,
        reference,
        value: "10k",
        footprint: "R_0603",
        fields: { Value: "10k", DNP: "No", "In BOM": "Yes" },
    };
}

function indexWith(catalog: AssemblyCatalogEntry[]): PrismSemanticIndex {
    return {
        schema: "prism.semantic_index_a0",
        sourceRevisionKey: "rev-a",
        components: [component("R1")],
        nets: [],
        terminals: [],
        indexes: {},
        assembly: {
            schema: "prism.assembly_state_a0",
            catalog,
            default: { occurrences: {}, components: {}, footprints: {} },
            variants: catalog.map((entry) => ({
                name: entry.name,
                occurrences: {},
                components: {},
                footprints: {},
            })),
            diagnostics: [],
        },
    };
}

function indexWithoutAssembly(): PrismSemanticIndex {
    return {
        schema: "prism.semantic_index_a0",
        sourceRevisionKey: "rev-old",
        components: [component("R1")],
        nets: [],
        terminals: [],
        indexes: {},
    };
}

const LITE: AssemblyCatalogEntry = {
    name: "Lite",
    description: null,
    sources: ["schematic"],
};

function catalog(
    overrides: Partial<VariantCatalogSnapshot> = {},
): VariantCatalogSnapshot {
    return {
        catalog: [LITE],
        loading: false,
        error: null,
        empty: false,
        identityMismatch: false,
        ...overrides,
    };
}

describe("requestedVariantFromSearchParams", () => {
    it("reads a non-empty variant parameter and treats blank as absent", () => {
        expect(
            requestedVariantFromSearchParams(
                new URLSearchParams("variant=Lite"),
            ),
        ).toBe("Lite");
        expect(
            requestedVariantFromSearchParams(new URLSearchParams("variant=")),
        ).toBeNull();
        expect(requestedVariantFromSearchParams(new URLSearchParams())).toBeNull();
    });
});

describe("variantSearchParams", () => {
    it("writes the variant while preserving the commit pin and open tab", () => {
        const current = new URLSearchParams("commit=abc123&tab=bom");
        const next = variantSearchParams(current, "Lite");
        expect(next.get("variant")).toBe("Lite");
        expect(next.get("commit")).toBe("abc123");
        expect(next.get("tab")).toBe("bom");
        expect(current.get("variant")).toBeNull();
    });

    it("deletes only the variant parameter for the default", () => {
        const current = new URLSearchParams("commit=abc123&variant=Lite");
        const next = variantSearchParams(current, null);
        expect(next.get("variant")).toBeNull();
        expect(next.get("commit")).toBe("abc123");
    });
});

describe("resolveVariantSelection", () => {
    it("stays loading until both the index and the catalog are in", () => {
        expect(
            resolveVariantSelection("Lite", catalog(), null).state,
        ).toBe("loading");
        expect(
            resolveVariantSelection("Lite", catalog({ loading: true }), indexWith([LITE])).state,
        ).toBe("loading");
    });

    it("reports failure, empty catalog and old index distinctly", () => {
        expect(
            resolveVariantSelection("Lite", catalog({ error: "boom" }), indexWith([LITE])).state,
        ).toBe("failed");
        expect(
            resolveVariantSelection(null, catalog({ catalog: [], empty: true }), indexWith([])).state,
        ).toBe("empty");
        expect(
            resolveVariantSelection(null, catalog(), indexWithoutAssembly()).state,
        ).toBe("unavailable");
    });

    it("advertises the default assembly without a request", () => {
        const resolution = resolveVariantSelection(null, catalog(), indexWith([LITE]));
        expect(resolution).toEqual({ effective: null, state: "applied" });
        expect(variantSelectionNotice(resolution)).toBeNull();
    });

    it("applies a name present in both catalogs", () => {
        const resolution = resolveVariantSelection("Lite", catalog(), indexWith([LITE]));
        expect(resolution).toEqual({ effective: "Lite", state: "applied" });
        expect(variantSelectionNotice(resolution, "Lite")).toBeNull();
    });

    it("reports a missing name and never falls back to it silently (E16)", () => {
        const resolution = resolveVariantSelection("Nope", catalog(), indexWith([LITE]));
        expect(resolution).toEqual({ effective: null, state: "missing" });
        expect(variantSelectionNotice(resolution, "Nope")).toContain("Nope");
    });

    it("keeps the missing notice even when the catalog is empty", () => {
        const resolution = resolveVariantSelection(
            "Nope",
            catalog({ catalog: [], empty: true }),
            indexWith([]),
        );
        expect(resolution.state).toBe("missing");
        expect(variantSelectionNotice(resolution, "Nope")).toContain("Nope");
    });

    it("does not apply a name the fetched catalog has but the index lacks", () => {
        const resolution = resolveVariantSelection(
            "Pro",
            catalog({ catalog: [LITE, { name: "Pro", description: null, sources: [] }] }),
            indexWith([LITE]),
        );
        expect(resolution.state).toBe("missing");
    });

    it("withholds the selection while catalog and index identities disagree (E17)", () => {
        const resolution = resolveVariantSelection(
            "Lite",
            catalog({ identityMismatch: true }),
            indexWith([LITE]),
        );
        expect(resolution).toEqual({ effective: null, state: "failed" });
    });
});

describe("selector presentation helpers", () => {
    it("disables the control only while nothing can be chosen yet", () => {
        expect(
            variantSelectorDisabled({ effective: null, state: "loading" }),
        ).toBe(true);
        expect(variantSelectorDisabled({ effective: null, state: "empty" })).toBe(true);
        expect(
            variantSelectorDisabled({ effective: null, state: "unavailable" }),
        ).toBe(true);
        expect(
            variantSelectorDisabled({ effective: "Lite", state: "applied" }),
        ).toBe(false);
        expect(
            variantSelectorDisabled({ effective: null, state: "missing" }),
        ).toBe(false);
        expect(variantSelectorDisabled({ effective: null, state: "failed" })).toBe(false);
    });
});
