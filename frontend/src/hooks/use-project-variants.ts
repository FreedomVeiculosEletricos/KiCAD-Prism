/**
 * Revision-owned design-variant catalog loading (VAR-12).
 *
 * The catalog is server state for one (project, revision) pair. This hook owns
 * exactly that request lifecycle: it loads, exposes loading/error/empty as
 * distinct outcomes, and drops any response whose project, revision or mount
 * no longer owns the request before publishing it. There is deliberately no
 * cache and no selected-variant state — the selection belongs to the URL
 * (VAR-14), and the effective projection is derived from the index (VAR-11).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { fetchJson } from "@/lib/api";
import type {
    AssemblyCatalogEntry,
    AssemblyDiagnostic,
} from "@/types/prism-selection";

export interface ProjectVariantsResponse {
    schema: "prism.project_variants_a0";
    projectId: string;
    commit: string | null;
    sourceRevisionKey: string;
    variants: AssemblyCatalogEntry[];
    diagnostics: AssemblyDiagnostic[];
}

export interface VariantCatalogIdentity {
    projectId: string;
    commit: string | null;
    sourceRevisionKey: string;
}

/**
 * The subset of the semantic index identity that has to agree before a catalog
 * and an index may be combined (packet 2.8). The index does not carry a
 * projectId; the request scope supplies it.
 */
export interface VariantIndexIdentity {
    commit?: string | null;
    sourceRevisionKey?: string;
}

export interface UseProjectVariantsOptions {
    projectId: string;
    /** Resolved commit SHA, or null for the working tree. */
    commit: string | null;
    /** Semantic-index identity to compare against, when one is loaded. */
    indexIdentity?: VariantIndexIdentity | null;
    /** Account scope: a response never crosses a sign-in change. */
    sessionKey?: string;
}

export interface ProjectVariantsState {
    catalog: AssemblyCatalogEntry[];
    diagnostics: AssemblyDiagnostic[];
    identity: VariantCatalogIdentity | null;
    loading: boolean;
    error: string | null;
    /** True only for a successful response with no variants anywhere. */
    empty: boolean;
    /** The catalog belongs to a different revision than the loaded index. */
    identityMismatch: boolean;
    reload: () => void;
}

interface Snapshot {
    scope: string;
    response: ProjectVariantsResponse | null;
    loading: boolean;
    error: string | null;
}

const EMPTY_CATALOG: AssemblyCatalogEntry[] = [];
const EMPTY_DIAGNOSTICS: AssemblyDiagnostic[] = [];

const isAbortError = (error: unknown): boolean =>
    error instanceof DOMException && error.name === "AbortError";

function scopeKeyFor(
    sessionKey: string,
    projectId: string,
    commit: string | null,
): string {
    return `${sessionKey}\u0000${projectId}\u0000${commit ?? ""}`;
}

function initialSnapshot(scope: string): Snapshot {
    return { scope, response: null, loading: true, error: null };
}

function catalogUrl(projectId: string, commit: string | null): string {
    const base = `/api/projects/${projectId}/variants`;
    return commit ? `${base}?commit=${encodeURIComponent(commit)}` : base;
}

export function useProjectVariants({
    projectId,
    commit,
    indexIdentity = null,
    sessionKey = "",
}: UseProjectVariantsOptions): ProjectVariantsState {
    const scope = scopeKeyFor(sessionKey, projectId, commit);
    const [snapshot, setSnapshot] = useState<Snapshot>(() =>
        initialSnapshot(scope),
    );
    const [reloadToken, setReloadToken] = useState(0);

    // The snapshot belongs to one scope; a scope change shows a fresh loading
    // state instead of mirroring the old catalog into this one.
    const current =
        snapshot.scope === scope ? snapshot : initialSnapshot(scope);

    const isMounted = useRef(true);
    const activeScope = useRef(scope);
    const latestRequest = useRef(0);

    // Every await boundary is followed by the owns() gate (mounted, scope and
    // request sequence), so a response nobody owns can never reach a setter;
    // the scanner cannot see that guard from outside the effect.
    // react-doctor-disable-next-line react-doctor/no-set-state-after-await-in-effect
    useEffect(() => {
        isMounted.current = true;
        activeScope.current = scope;
        const controller = new AbortController();
        const sequence = ++latestRequest.current;
        const owns = () =>
            isMounted.current &&
            activeScope.current === scope &&
            latestRequest.current === sequence &&
            !controller.signal.aborted;

        // A reload keeps the previous catalog visible until the new response
        // lands; a scope change starts from loading.
        setSnapshot((previous) => {
            const base =
                previous.scope === scope ? previous : initialSnapshot(scope);
            return { ...base, loading: true, error: null };
        });

        const load = async () => {
            try {
                const response = await fetchJson<ProjectVariantsResponse>(
                    catalogUrl(projectId, commit),
                    { signal: controller.signal },
                    "Failed to load design variants",
                );
                if (!owns()) return;
                setSnapshot({ scope, response, loading: false, error: null });
            } catch (error) {
                if (!owns() || isAbortError(error)) return;
                setSnapshot({
                    scope,
                    response: null,
                    loading: false,
                    error:
                        error instanceof Error
                            ? error.message
                            : "Failed to load design variants",
                });
            }
        };
        void load();

        return () => {
            isMounted.current = false;
            controller.abort();
        };
    }, [commit, projectId, reloadToken, scope]);

    const reload = useCallback(() => setReloadToken((token) => token + 1), []);

    const identity = useMemo<VariantCatalogIdentity | null>(() => {
        const response = current.response;
        if (!response) return null;
        return {
            projectId: response.projectId,
            commit: response.commit ?? null,
            sourceRevisionKey: response.sourceRevisionKey,
        };
    }, [current.response]);

    const identityMismatch = useMemo(() => {
        if (!identity || !indexIdentity) return false;
        if (
            indexIdentity.commit !== undefined &&
            (indexIdentity.commit ?? null) !== identity.commit
        ) {
            return true;
        }
        return (
            indexIdentity.sourceRevisionKey !== undefined &&
            indexIdentity.sourceRevisionKey !== identity.sourceRevisionKey
        );
    }, [identity, indexIdentity]);

    return {
        catalog: current.response?.variants ?? EMPTY_CATALOG,
        diagnostics: current.response?.diagnostics ?? EMPTY_DIAGNOSTICS,
        identity,
        loading: current.loading,
        error: current.error,
        empty:
            !current.loading &&
            current.error === null &&
            current.response !== null &&
            current.response.variants.length === 0,
        identityMismatch,
        reload,
    };
}
