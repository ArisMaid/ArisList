/**
 * Compare the source identity captured by capture-baseline with the source
 * identity captured by the Gate orchestrator.  captureSourceProvenance returns
 * a flat object; baseline artifacts wrap that object under `source`.
 */
export function sourceProvenanceMatches(baselineSource, currentSource) {
  if (!baselineSource || !currentSource) return false;
  return baselineSource.commit === currentSource.commit
    && baselineSource.dirty === currentSource.dirty
    && baselineSource.dirty_state_sha256 === currentSource.dirty_state_sha256;
}
