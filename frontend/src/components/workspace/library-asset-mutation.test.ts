import { describe, expect, it } from "vitest";

import { assetMutationRevisionId } from "./library-asset-mutation";

describe("assetMutationRevisionId", () => {
  it("keeps the first selection_required revision after the loaded component moves on", () => {
    expect(assetMutationRevisionId("rev-later", "rev-first")).toBe("rev-first");
  });

  it("uses the loaded revision when there is no retained picker state", () => {
    expect(assetMutationRevisionId("rev-current")).toBe("rev-current");
    expect(assetMutationRevisionId("rev-current", "")).toBe("rev-current");
  });
});
