const unsupportedPdfJs = {
  GlobalWorkerOptions: {} as { workerSrc?: string },
  getDocument() {
    throw new Error("PDF rendering is not enabled in the ArisList EPUB reader");
  }
};

const pdfGlobal = globalThis as typeof globalThis & { pdfjsLib?: typeof unsupportedPdfJs };
pdfGlobal.pdfjsLib ??= unsupportedPdfJs;

export {};
