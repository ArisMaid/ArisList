export type LibraryViewMode = "cover" | "grid" | "compact" | "list";
export type LibraryContentKind = "portrait" | "audio";

export type LibraryLayout = {
  columns: number;
  gap: number;
  columnWidth: number;
  coverRatio: number;
  textReserve: number;
  cardHeight: number;
  rowHeight: number;
};

const targetWidths: Record<Exclude<LibraryViewMode, "list">, number> = {
  cover: 224,
  grid: 184,
  compact: 144
};

/**
 * Keep the virtual shelf's geometry in one place.  CSS consumes the same
 * values through custom properties so a resize cannot make the browser grid
 * and the windowing math disagree about where an item lives.
 */
export function getLibraryLayout(
  viewMode: LibraryViewMode,
  contentWidth: number,
  contentKind: LibraryContentKind = "portrait"
): LibraryLayout {
  const width = Math.max(1, Number.isFinite(contentWidth) && contentWidth > 0 ? contentWidth : 960);
  const gap = viewMode === "list" ? 12 : 16;

  if (viewMode === "list") {
    const cardHeight = 104;
    return {
      columns: 1,
      gap,
      columnWidth: width,
      coverRatio: 1,
      textReserve: 0,
      cardHeight,
      rowHeight: cardHeight + gap
    };
  }

  const targetWidth = targetWidths[viewMode];
  const columns = Math.max(1, Math.floor((width + gap) / (targetWidth + gap)));
  const columnWidth = Math.max(120, (width - gap * (columns - 1)) / columns);
  const coverRatio = contentKind === "audio" ? 1 : 4 / 3;
  const textReserve = viewMode === "cover" ? 76 : viewMode === "compact" ? 82 : 86;
  const cardHeight = Math.ceil(columnWidth * coverRatio + textReserve);

  return {
    columns,
    gap,
    columnWidth,
    coverRatio,
    textReserve,
    cardHeight,
    rowHeight: cardHeight + gap
  };
}
