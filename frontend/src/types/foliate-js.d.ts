declare module "foliate-js/epub.js" {
  export class EPUB {
    constructor(loader: {
      entries?: Array<{ filename: string }>;
      loadText: (name: string) => Promise<string | null> | string | null;
      loadBlob: (name: string, type?: string) => Promise<Blob | null> | Blob | null;
      getSize: (name: string) => number;
    });
    init(): Promise<FoliateBookDoc>;
  }
}

declare module "foliate-js/view.js" {}

type FoliateTOCItem = {
  id?: number;
  label?: string;
  href?: string;
  subitems?: FoliateTOCItem[];
};

type FoliateSectionItem = {
  id?: string;
  cfi?: string;
  size?: number;
  linear?: string;
  href?: string;
  createDocument?: () => Promise<Document>;
  load?: () => Promise<string> | string;
  unload?: () => void;
};

type FoliateBookDoc = {
  metadata?: {
    title?: string | Record<string, string>;
    language?: string | string[];
  };
  rendition?: {
    layout?: "pre-paginated" | "reflowable";
    spread?: "auto" | "none";
  };
  dir?: string;
  toc?: FoliateTOCItem[];
  pageList?: FoliateTOCItem[];
  sections: FoliateSectionItem[];
  transformTarget?: EventTarget;
  splitTOCHref?: (href: string) => Array<string | number> | Promise<Array<string | number>>;
  getTOCFragment?: (doc: Document, id: unknown) => Node | null;
  getCover?: () => Promise<Blob | null>;
  resolveHref?: (href: string) => unknown;
  resolveCFI?: (cfi: string) => unknown;
};

type FoliateLocation = {
  current: number;
  next?: number;
  total: number;
};

type FoliateRelocateDetail = {
  cfi?: string;
  fraction?: number;
  index?: number;
  tocItem?: FoliateTOCItem | null;
  pageItem?: FoliateTOCItem | null;
  section?: FoliateLocation;
  location?: FoliateLocation;
  time?: {
    section?: number;
    total?: number;
  };
  range?: Range;
};

type FoliateSearchExcerpt = {
  pre?: string;
  match?: string;
  post?: string;
};

type FoliateSearchSubitem = {
  cfi: string;
  excerpt?: FoliateSearchExcerpt;
};

type FoliateSearchResult =
  | "done"
  | { progress: number }
  | { label?: string; subitems: FoliateSearchSubitem[] }
  | FoliateSearchSubitem;

type FoliateView = HTMLElement & {
  book?: FoliateBookDoc;
  renderer: HTMLElement & {
    containerPosition?: number;
    size?: number;
    setStyles?: (styles: string | [string, string]) => void;
    getContents?: () => Array<{ doc?: Document; index?: number }>;
    scrollBy?: (dx: number, dy: number) => void;
    snap?: (vx: number, vy: number) => void;
    setAttribute: HTMLElement["setAttribute"];
    removeAttribute: HTMLElement["removeAttribute"];
  };
  open: (book: FoliateBookDoc | Blob | File | string) => Promise<void>;
  init: (options: { lastLocation?: string | number; showTextStart?: boolean }) => Promise<void>;
  goTo: (target: string | number | { fraction: number }) => Promise<unknown>;
  goToFraction: (fraction: number) => Promise<void>;
  prev: (distance?: number) => Promise<void>;
  next: (distance?: number) => Promise<void>;
  goLeft: () => Promise<void>;
  goRight: () => Promise<void>;
  close: () => void;
  search: (options: { query: string; index?: number }) => AsyncGenerator<FoliateSearchResult>;
  clearSearch: () => void;
};
