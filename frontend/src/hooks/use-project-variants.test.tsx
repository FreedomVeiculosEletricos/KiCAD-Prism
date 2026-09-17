/**
 * VAR-12: the catalog hook owns one (project, revision, session) request and
 * publishes only the response that still owns it.
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
    fetchJson: vi.fn(),
}));

vi.mock("@/lib/api", () => api);

import { useProjectVariants } from "./use-project-variants";
import type { ProjectVariantsResponse } from "./use-project-variants";

interface Deferred<T> {
    promise: Promise<T>;
    resolve: (value: T) => void;
    reject: (error: unknown) => void;
    url: string;
    signal?: AbortSignal;
}

function queueLoads() {
    const loads: Deferred<unknown>[] = [];
    api.fetchJson.mockImplementation(
        (input: RequestInfo | URL, init?: RequestInit) => {
            let resolve!: (value: unknown) => void;
            let reject!: (error: unknown) => void;
            const promise = new Promise<unknown>((res, rej) => {
                resolve = res;
                reject = rej;
            });
            loads.push({
                promise,
                resolve,
                reject,
                url: String(input),
                signal: init?.signal ?? undefined,
            });
            return promise;
        },
    );
    return loads;
}

function catalog(
    tag: string,
    overrides: Partial<ProjectVariantsResponse> = {},
): ProjectVariantsResponse {
    return {
        schema: "prism.project_variants_a0",
        projectId: "prj",
        commit: "sha-a",
        sourceRevisionKey: `key-${tag}`,
        variants: [
            { name: `Variant-${tag}`, description: null, sources: ["project"] },
        ],
        diagnostics: [],
        ...overrides,
    };
}

async function flush() {
    await act(async () => {
        await Promise.resolve();
        await Promise.resolve();
    });
}

beforeEach(() => {
    api.fetchJson.mockReset();
});

afterEach(() => {
    vi.restoreAllMocks();
});

describe("useProjectVariants", () => {
    it("loads the revision catalog and reports its identity", async () => {
        const loads = queueLoads();
        const { result } = renderHook(() =>
            useProjectVariants({ projectId: "prj", commit: "sha-a" }),
        );

        expect(loads[0]!.url).toBe("/api/projects/prj/variants?commit=sha-a");
        expect(loads[0]!.signal).toBeDefined();
        expect(result.current.loading).toBe(true);
        expect(result.current.empty).toBe(false);

        await act(async () => loads[0]!.resolve(catalog("a")));
        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBeNull();
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-a",
        ]);
        expect(result.current.identity).toEqual({
            projectId: "prj",
            commit: "sha-a",
            sourceRevisionKey: "key-a",
        });
    });

    it("omits the commit parameter for the working tree and reloads it", async () => {
        const loads = queueLoads();
        const { result } = renderHook(() =>
            useProjectVariants({ projectId: "prj", commit: null }),
        );
        expect(loads[0]!.url).toBe("/api/projects/prj/variants");

        await act(async () =>
            loads[0]!.resolve(
                catalog("wt", { commit: null, sourceRevisionKey: "key-wt" }),
            ),
        );
        expect(result.current.identity).toEqual({
            projectId: "prj",
            commit: null,
            sourceRevisionKey: "key-wt",
        });

        act(() => result.current.reload());
        expect(loads).toHaveLength(2);
        await act(async () =>
            loads[1]!.resolve(
                catalog("wt2", { commit: null, sourceRevisionKey: "key-wt2" }),
            ),
        );
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-wt2",
        ]);
        expect(result.current.identity?.sourceRevisionKey).toBe("key-wt2");
    });

    it("keeps a failed request distinct from an empty catalog", async () => {
        const loads = queueLoads();
        const { result } = renderHook(() =>
            useProjectVariants({ projectId: "prj", commit: "sha-a" }),
        );

        await act(async () => loads[0]!.reject(new Error("catalog unavailable")));
        expect(result.current.error).toBe("catalog unavailable");
        expect(result.current.loading).toBe(false);
        expect(result.current.empty).toBe(false);
        expect(result.current.catalog).toEqual([]);

        act(() => result.current.reload());
        await act(async () =>
            loads[1]!.resolve(catalog("retry", { variants: [] })),
        );
        expect(result.current.error).toBeNull();
        expect(result.current.empty).toBe(true);
    });

    it("a late response after a revision change cannot publish", async () => {
        const loads = queueLoads();
        const { result, rerender } = renderHook(
            (props: { commit: string }) =>
                useProjectVariants({ projectId: "prj", commit: props.commit }),
            { initialProps: { commit: "sha-a" } },
        );

        rerender({ commit: "sha-b" });
        await flush();

        // The old revision's response arrives after the switch.
        await act(async () => loads[0]!.resolve(catalog("a")));
        expect(result.current.catalog).toEqual([]);
        expect(result.current.identity).toBeNull();

        expect(loads[1]!.url).toBe("/api/projects/prj/variants?commit=sha-b");
        await act(async () =>
            loads[1]!.resolve(
                catalog("b", { commit: "sha-b", sourceRevisionKey: "key-b" }),
            ),
        );
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-b",
        ]);
        expect(result.current.identity?.commit).toBe("sha-b");
    });

    it("a stale response cannot overwrite a newer reload", async () => {
        const loads = queueLoads();
        const { result } = renderHook(() =>
            useProjectVariants({ projectId: "prj", commit: "sha-a" }),
        );

        act(() => result.current.reload());
        expect(loads).toHaveLength(2);
        await act(async () => loads[1]!.resolve(catalog("new")));
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-new",
        ]);

        await act(async () => loads[0]!.resolve(catalog("old")));
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-new",
        ]);
    });

    it("a sign-in change never publishes the previous session's catalog", async () => {
        const loads = queueLoads();
        const { result, rerender } = renderHook(
            (props: { sessionKey: string }) =>
                useProjectVariants({
                    projectId: "prj",
                    commit: "sha-a",
                    sessionKey: props.sessionKey,
                }),
            { initialProps: { sessionKey: "alice" } },
        );

        rerender({ sessionKey: "bob" });
        await flush();
        await act(async () => loads[0]!.resolve(catalog("alice")));
        expect(result.current.catalog).toEqual([]);

        await act(async () => loads[1]!.resolve(catalog("bob")));
        expect(result.current.catalog.map((entry) => entry.name)).toEqual([
            "Variant-bob",
        ]);
    });

    it("discards an in-flight response after unmount", async () => {
        const loads = queueLoads();
        const { unmount } = renderHook(() =>
            useProjectVariants({ projectId: "prj", commit: "sha-a" }),
        );

        unmount();
        expect(loads[0]!.signal?.aborted).toBe(true);
        await act(async () => loads[0]!.resolve(catalog("gone")));
    });

    it("flags an index identity mismatch and accepts a matching one", async () => {
        const loads = queueLoads();
        type IdentityProps = {
            indexIdentity: {
                commit?: string | null;
                sourceRevisionKey?: string;
            };
        };
        const matching: IdentityProps = {
            indexIdentity: { commit: "sha-a", sourceRevisionKey: "key-a" },
        };
        const { result, rerender } = renderHook(
            (props: IdentityProps) =>
                useProjectVariants({
                    projectId: "prj",
                    commit: "sha-a",
                    indexIdentity: props.indexIdentity,
                }),
            { initialProps: matching },
        );
        await act(async () => loads[0]!.resolve(catalog("a")));
        expect(result.current.identityMismatch).toBe(false);

        rerender({
            indexIdentity: { commit: "sha-a", sourceRevisionKey: "key-other" },
        });
        expect(result.current.identityMismatch).toBe(true);

        rerender({ indexIdentity: { commit: "sha-b" } });
        expect(result.current.identityMismatch).toBe(true);

        rerender({ indexIdentity: { sourceRevisionKey: "key-a" } });
        expect(result.current.identityMismatch).toBe(false);
    });
});
