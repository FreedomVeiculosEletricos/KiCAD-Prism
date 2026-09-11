/** Revision sent with catalog asset upload/link requests.

Omitted/empty is the backend's legacy skip. Current editors always send one.
A selection_required picker keeps the revision from the first POST so a later
loaded head cannot silently attach. A 409 releases that retained id so a
refresh can retry against the new head without closing the picker.
*/
export function assetMutationRevisionId(
  currentRevisionId: string,
  retainedFromSelection?: string,
): string {
  return retainedFromSelection || currentRevisionId;
}

export function releaseRetainedRevisionOnConflict<T extends { expectedRevisionId: string }>(
  selection: T | null,
  status: number | undefined,
): T | null {
  if (!selection || status !== 409) return selection;
  return { ...selection, expectedRevisionId: "" };
}
