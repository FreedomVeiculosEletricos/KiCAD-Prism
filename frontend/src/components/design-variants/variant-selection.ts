/**
 * URL-owned design-variant selection (VAR-14).
 *
 * The requested variant lives in `?variant=<name>` and nowhere else: these
 * helpers read it, rewrite it while preserving every other query parameter,
 * and decide whether the loaded catalog and index can actually honour it
 * before the UI advertises a named selection (packet 3.6, cases E16/E17).
 */

import { assemblyProjectionState } from "@/lib/design-variants";
import type {
    AssemblyCatalogEntry,
    PrismSemanticIndex,
} from "@/types/prism-selection";

export type VariantSelectionState =
    | "applied"
    | "missing"
    | "empty"
    | "loading"
    | "failed"
    | "unavailable";

export interface VariantSelectionResolution {
    /** The name the index may render, or null for the default assembly. */
    effective: string | null;
    state: VariantSelectionState;
}

/**
 * The catalog fields the resolution reads. Deliberately structural so the
 * visualizer can pass the hook's state without copying it into its own state.
 */
export interface VariantCatalogSnapshot {
    catalog: readonly AssemblyCatalogEntry[];
    loading: boolean;
    error: string | null;
    empty: boolean;
    identityMismatch: boolean;
}

export function requestedVariantFromSearchParams(
    searchParams: URLSearchParams,
): string | null {
    const value = searchParams.get("variant");
    return value ? value : null;
}

/**
 * Rewrite only the variant parameter. Every other query parameter survives,
 * including the commit pin and the open tab.
 */
export function variantSearchParams(
    searchParams: URLSearchParams,
    name: string | null,
): URLSearchParams {
    const next = new URLSearchParams(searchParams);
    if (name) next.set("variant", name);
    else next.delete("variant");
    return next;
}

/**
 * Decide what the current revision can render. Catalogue and index identities
 * must agree and the name must exist in both the fetched catalog and the
 * index's own catalog before `applied` is reported; a named request never
 * falls through to a silently rendered default.
 */
export function resolveVariantSelection(
    requested: string | null,
    catalog: VariantCatalogSnapshot,
    index: PrismSemanticIndex | null,
): VariantSelectionResolution {
    if (index === null || catalog.loading) {
        return { effective: null, state: "loading" };
    }
    if (catalog.error) {
        return { effective: null, state: "failed" };
    }
    if (catalog.identityMismatch) {
        return { effective: null, state: "failed" };
    }
    if (assemblyProjectionState(index, null) === "unavailable") {
        return { effective: null, state: "unavailable" };
    }
    if (!requested) {
        return { effective: null, state: catalog.empty ? "empty" : "applied" };
    }
    const known =
        catalog.catalog.some((variant) => variant.name === requested) &&
        assemblyProjectionState(index, requested) === "applied";
    return known
        ? { effective: requested, state: "applied" }
        : { effective: null, state: "missing" };
}

/**
 * The one-line explanation rendered beside the selector. `applied` and
 * `loading` need none: the control itself already shows both states.
 */
export function variantSelectionNotice(
    resolution: VariantSelectionResolution,
    requested: string | null = null,
): string | null {
    switch (resolution.state) {
        case "missing":
            return `Variant "${requested ?? "?"}" is not in this revision; showing the default assembly.`;
        case "empty":
            return "This revision has no design variants.";
        case "failed":
            return "Design variants could not be loaded.";
        case "unavailable":
            return "This revision has no variant data; showing the default assembly.";
        case "loading":
            return "Loading design variants…";
        case "applied":
            return null;
    }
}

export function variantSelectorDisabled(
    resolution: VariantSelectionResolution,
): boolean {
    return (
        resolution.state === "loading" ||
        resolution.state === "empty" ||
        resolution.state === "unavailable"
    );
}
