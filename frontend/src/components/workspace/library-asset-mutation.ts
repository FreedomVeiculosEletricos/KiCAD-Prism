/** Revision sent with catalog asset upload/link requests.

Omitted/empty is the backend's legacy skip. Current editors always send one.
A selection_required picker keeps the revision from the first POST so a later
loaded head cannot silently attach.
*/
export function assetMutationRevisionId(
  currentRevisionId: string,
  retainedFromSelection?: string,
): string {
  return retainedFromSelection || currentRevisionId;
}
