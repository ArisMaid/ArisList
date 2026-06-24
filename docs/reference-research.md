# Reference Research

Last checked: 2026-06-11

This project focuses on a private local bookshelf for comics, light novels, audio, and gallery folders. The notes below record the current reference points used by the implementation so future changes can be checked against the same product shape.

## Tag Translation

Sources:

- https://github.com/EhTagTranslation/Database
- https://fastly.jsdelivr.net/gh/EhTagTranslation/DatabaseReleases/db.html.json

Observed and adopted patterns:

- The upstream database is namespace-oriented: `female`, `male`, `mixed`, `language`, `artist`, `group`, `parody`, `character`, `cosplayer`, `location`, `other`, and reclassification namespaces are independent vocabularies.
- Translated tag labels, translated namespaces, intros, and links should be retained separately from raw namespace keys.
- The local model should preserve raw namespace/key pairs as the canonical identity, with translated labels used only as display/search metadata.

Implementation hooks:

- Backend import job: `import-tag-translations`.
- DB fields: `namespace`, `key`, `label`, `translated_label`, `translated_namespace`, `source`, `intro`, `links`.
- Frontend controls: include/exclude tag cycle, tag detail panel, raw/translated label toggle, shared tag tree across all shelf kinds.

## LightNovel.app / LightNovelShelf

Source:

- https://www.lightnovel.app/book/rank/monthly

Chrome observations:

- The monthly rank page uses a compact Quasar-style app layout with top navigation for home, announcements, all novels, recent rank, shelf, community, history, settings, and contribution list.
- The page exposes search and category inputs, book-cover cards, status chips such as completion/translation state, and book title text in dense ranked rows.
- Tag-like metadata is lightweight on the rank page itself; deeper book data is expected from book detail and tag APIs.

Implementation hooks:

- Backend enrichment attempts SignalR methods such as `GetBookInfo` and `GetBookListByTags`, with EPUB OPF metadata as fallback.
- Local novel tags use `ln:*` for subjects and keep shared namespaces such as `series`, `artist`, and `language`.
- Frontend `ShelfLensPanel` mirrors the monthly-rank feel with ranked rows, compact book metadata, and local tag chips instead of a marketing page.

## ASMR One

Source:

- https://asmr.one/works

Chrome observations:

- The works page uses a navigation rail for media library, favorites, playlist, circles, tags, voice actors, about, and settings.
- Work cards carry RJ code, release date, title, tag chips, circle, voice actors, playback duration, subtitles/language indicators, and playlist actions.
- The useful local model is tag-heavy: audio tags, circle, voice actors, series, track tree, and preferred playback variants.

Implementation hooks:

- Backend enrichment reads `workInfo`, `work`, and `tracks` endpoints by RJ id.
- Audio scanning groups folders by RJ number, writes a stable `track_key` for same-named MP3/WAV files, keeps MP3 as preferred playback where available, and retains WAV/other lossless tracks as variants.
- Local audio tags use `audio:*`, `circle`, `va`, `series`, and `source` namespaces.
- Frontend audio lens uses ASMR-style dense cards, tag chips, track counts/duration metadata, and a mini player.

## UI And Motion Decisions

- The first screen is the actual bookshelf app, not a landing page.
- The layout stays three-pane on desktop: navigation/tag tree, virtualized work grid/list, detail/queue/player pane.
- Mobile collapses into a single-column app surface without horizontal overflow.
- Animations are limited to transform and opacity: card stagger, detail panel transition, filter chip collapse, reader page transition, and audio dock slide.
- Generated UI assets are safe non-explicit backgrounds, empty states, and placeholder covers. Runtime generations are saved locally and registered under a generated asset shelf item.
