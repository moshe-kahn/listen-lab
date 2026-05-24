# Source-Track Resolution Policy

This policy defines read-only review rules for ambiguous Spotify source tracks before any promotion, apply, merge, or queue-rebuild flow exists.

Preferred product wording is Track Family. Current code may still use `analysis_track` for that layer.

## Scope

This applies to Spotify `source_track` ambiguity where multiple Spotify track IDs may represent:

- the same `release_track`
- separate release tracks within the same album family
- separate tracks that still map to the same Track Family
- unresolved cases where available evidence is insufficient

This policy is evidence and review guidance only. It does not authorize schema changes, Spotify fetches, queue clearing, or identity-table mutations.

## Identity Layers

- `source_track`: provider/source identity. One Spotify track ID remains one source row.
- `release_track`: ListenLab release-level track identity. Multiple Spotify source tracks may map here when they appear to be the same recording in the same album-track role.
- Track Family (`analysis_track` in current code): broader song or analysis grouping. Alternate recordings, demos, remixes, live versions, and clean/explicit variants may relate here even when they should not collapse into one release track.
- Album family: related album/release variants. Deluxe, regional, reissue, and label variants may belong to one family while remaining separate album releases.
- Representative display metadata: derived display values chosen from evidence, not a blind copy of one Spotify album or track.

Current UI direction:
- release-track identity is the preferred same-song identity for liked/display state.
- Activity/Listened display grouping prefers `release_track_id`, then falls back to Spotify track id, then normalized text identity.
- Spotify track IDs remain concrete playback and provider-version identifiers.
- Track Family should not be used for playback substitution without returning a playable representative Spotify URI.
- Future related-album UI should first expose read-only related album appearances from the backend, with exact release-track siblings before broader Track Family matches.

## Classification Checklist

Review each ambiguous album/source-track group with the strongest available evidence first: album shape, track position, duration, title/artist semantics, explicit/version flags, and external IDs.

### Exact Duplicate

Classify as same release track when source tracks agree on:

- same or equivalent primary artist
- same album/release context
- same disc number and track number
- same track name after normal normalization
- same or near-identical `duration_ms`
- no meaningful version flags

Different Spotify IDs alone are not enough to split identity.

### Casing, Punctuation, Or Spacing Variant

Classify as same release track when differences are limited to casing, punctuation, whitespace, or harmless formatting.

Examples:

- `Intro` vs `intro`
- `Timelapse - Walrus` vs `Timelapse (Walrus)`
- `2:20 Am` vs `2: 20 Am`

These differences should influence representative title selection, not identity splitting.

### Transliteration Or Language Title Variant

Classify as same release track when title language or transliteration differs but the surrounding evidence agrees:

- same artist
- same album family/release context
- same disc and track position
- same or near-identical duration
- no version/remix/live/demo signal

Keep all observed title forms as provenance. Representative title selection can prefer the clearest user-facing title later.

### Feature Or Co-Artist Attribution Variant

Classify as same release track when the only meaningful difference is where the same feature/co-artist credit appears:

- in the title
- in the artist list
- omitted from one source but present in another
- spelled or normalized differently

Escalate to review when the extra artist appears to indicate a materially different version, remix, duet, edit, or rerecording rather than a metadata-credit difference.

### Label, Copyright, Or Regional Variant

Treat label, copyright, and regional metadata as low-weight identity evidence.

Classify as same release track when content evidence agrees and the only notable differences are:

- label
- copyright text
- distributor
- market/territory release
- release date drift consistent with regional publication or reissue metadata

Escalate to review when label/copyright differences arrive with content differences such as extra tracks, unavailable tracks, changed durations, remaster/demo/video flags, or changed album shape.

### Availability Variant

Do not split track identity only because one Spotify source track or sibling album track is unavailable.

Use availability as a review signal:

- Same metadata but different availability can still be same release track.
- Large unavailable sections may block confidence because album shape cannot be fully compared.
- Availability differences across album variants should be preserved as provenance.

Classify as unresolved when unavailable tracks prevent comparison and no stronger evidence exists.

### Deluxe Or Bonus-Track Album Variant

Classify albums as same album family but separate album releases when one version adds deluxe, demo, bonus, or expanded tracks.

For overlapping original-album tracks:

- classify as same release track when track-level evidence agrees
- keep bonus/demo/live/remix tracks separate release tracks
- map related alternate versions to the same Track Family only when the track-variant policy supports it

Do not choose the original or deluxe album as a global canonical album by default.

### Explicit, Clean, Video, Demo, Or Remix Variant

Treat these as identity-affecting signals.

Default handling:

- explicit vs clean: usually same Track Family, review before same release track
- video version: usually separate release track, same Track Family only if audio/content lineage supports it
- demo: separate release track, usually same Track Family
- remix/rework: separate release track, usually same Track Family only when policy allows derived-version grouping
- live/acoustic/session/version/edit/mix labels: use the track-variant policy and escalate when overloaded wording is ambiguous

Classify as same release track only when evidence shows the flag is a metadata annotation and not a materially different listening object.

### Duration Mismatch

Small `duration_ms` differences can be metadata noise, but duration is stronger than label/copyright evidence.

Review rules:

- same playback length despite Spotify metadata drift can still be same release track
- one-to-two second differences require stronger corroborating evidence
- larger differences require playback evidence, ISRC match, or other strong evidence
- consistent duration differences across many sibling tracks may indicate systematic provider metadata drift
- isolated large duration differences should remain unresolved or separate until explained

When playback evidence is used, record that it came from manual review and should be treated as reviewer evidence, not catalog metadata.

### Unresolved

Classify as unresolved when evidence is incomplete or contradictory.

Common unresolved reasons:

- missing track metadata
- missing full album metadata
- missing album tracklists
- unavailable tracks block album-shape comparison
- unexplained duration differences
- different track positions with no clear reissue/deluxe explanation
- artist/title differences may indicate alternate recording or attribution drift
- no external IDs or other corroborating evidence

Unresolved cases should not be promoted by confidence alone.

## Metadata Evidence Requirements

Each ambiguous album/source-track group should have enough Spotify evidence to compare album shape and individual track identity.

Required evidence:

- full album metadata for every candidate album
- full tracklist for every candidate album
- track metadata for every track in those tracklists
- track availability/playability when present
- `duration_ms`
- explicit flag
- artists
- track name
- disc number
- track number
- album images
- copyrights
- label
- release date
- total tracks
- external IDs where available, especially ISRC

Album-shape comparison should include sibling tracks, not only the ambiguous track, because extra tracks, missing tracks, deluxe editions, and regional variants are album-level evidence.

## Label And Copyright Policy

Label and copyright are provenance first and identity evidence second.

Rules:

- Do not split track identity only because label/copyright differs.
- Preserve all observed label/copyright values as deduped provenance.
- For representative display metadata, prefer the most recent complete album metadata only when it does not conflict with stronger content evidence.
- If copyright/label differences come with content differences, escalate to review.

Content differences include:

- extra tracks
- unavailable tracks
- remaster/demo/video flags
- explicit/clean changes
- changed durations
- changed album shape

Representative metadata should remain rule-derived and revisable. A future display layer may choose album art from one source, title from another, co-artist attribution from another, and release/copyright metadata from the most complete compatible source.

## Reviewed Example Test Cases

Manual review examples should be recorded as regression-style policy test cases before any apply/promote flow exists.

Each test case should record:

- artist name
- album release year or years
- album name
- involved Spotify album IDs when known
- involved Spotify source track IDs when known
- observed differences
- missing evidence
- expected classification
- reviewer rationale
- confidence state: confirmed, needs review, or unresolved

Expected classification should answer:

- Which groups would auto-classify as same release track?
- Which groups become same album family, separate album releases?
- Which tracks stay separate but map to the same Track Family?
- Which remain unresolved because metadata is insufficient?

Initial reviewed patterns to preserve as test fixtures:

- `jizue - Bookshelf`: same label and length; casing and language/title variants. Expected to test same-release-track classification when track position and duration agree.
- `BADBADNOTGOOD - III`: label differences and featured-credit placement differences. Expected to test co-artist/title attribution policy.
- `Teleskopes - Stereocilia`: spacing-only title difference. Expected to test formatting-only normalization.
- `Oliver Coates - Upsetting`: punctuation/title-style difference. Expected to test formatting-only normalization.
- `Laurence Guy - Saw You for the First Time`: co-artist spelling/normalization difference. Expected to test co-artist attribution policy.
- `Animal Collective - Feels`: base album variants plus deluxe edition. Expected to test same album family with separate album releases and overlapping same-release-track handling.
- `Sharon Van Etten - Tramp`: deluxe edition with added demo versions. Expected to test original tracks vs demo tracks.
- `Connan Mockasin - Jassbusters`: explicit-song and video-linked variants. Expected to test identity-affecting variant flags.
- `Avishai Cohen - Seven Seas`: mostly duplicates plus one label/regional version with an additional song. Expected to test label/regional differences and album-shape differences.
- `Peter Broderick - Earnest Leslye`: pairs of separate labels. Expected to test low-weight label handling.
- `BLESS Vol. 3 - ideism`: many unavailable tracks with availability differences across versions. Expected to test unresolved or review-required availability cases.
- `Dylan LeBlanc - Cautionary Tale`: metadata duration drift where manual playback suggested same length. Expected to test duration-mismatch review.
- `Fatoumata Diawara - Fatou`: different lengths without resolved explanation. Expected to remain unresolved until stronger evidence exists.
- `Ill Considered - Ill Considered`: release-year and artwork differences. Expected to test album-family grouping with separate release metadata.
- `Funkadelic - Maggot Brain`: track-count and availability differences. Expected to test album-shape comparison under availability gaps.
- `Boom Pam - Manara`: artist attribution, release-year, and track-count differences. Expected to test album-family and track-level case-by-case review.
- `Mdou Moctar - Sousoume Tamachek`: album-art difference. Expected to test low-weight artwork handling when content agrees.
- `Other Lives - Tamer Animals`: extra unavailable song. Expected to test album-shape differences where availability limits confidence.

The reviewed set should be run against the policy as a read-only evaluation before designing any apply, promote, or write path.

## Queue Policy For Resolution Evidence

`resolution_evidence` is a temporary focused queue reason concept. It is not generic catalog backfill.

Its purpose is to fetch only the albums and tracks needed to evaluate source-to-release ambiguity groups, including sibling tracks on the same albums. This lets reviewers compare album shape instead of isolated tracks.

Expected target evidence:

- candidate albums for ambiguous source-track groups
- full tracklists for those candidate albums
- track metadata for every track in those tracklists
- album metadata needed for artwork, label, copyright, release date, and total-track comparison

Recommended order:

1. Write the policy/checklist doc.
2. Define metadata requirements.
3. Inspect the current queue and classify what is already relevant.
4. Only then decide whether to preserve, pause, clear, or replace the queue with a focused `resolution_evidence` batch.

Do not clear or rebuild the current Spotify queue until the target set and preservation strategy are explicit.

## Non-Goals

Do not add as part of this policy:

- schema changes
- queue clearing
- Spotify fetches
- apply/promote/merge behavior
- backend endpoints
- automatic canonical album selection
- automatic canonical source-track selection
- identity mutation from catalog metadata alone
