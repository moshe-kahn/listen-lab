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
- `release_track`: ListenLab release-level track identity. Multiple Spotify source tracks may map here when they appear to be the same recording in the same album-track role or a near-release cluster with only metadata drift.
- `recording_track` (proposed layer): normal listening identity for the same core recorded performance or master lineage across releases. This can group separate `release_track` rows for album version, single release, compilation appearance, soundtrack appearance, rerelease, remaster, and mono/stereo cases when evidence supports same core recording. It should not include materially different performances or arrangements.
- Track Family (`analysis_track` in current code): broader song or analysis grouping above `recording_track`. Radio edits, alternate takes, demos, acoustic versions, live versions, remixes, rerecordings, structural segments, and instrumental variants may relate here even when they should not collapse into one `recording_track`.
- Album family: related album/release variants. Deluxe, regional, reissue, and label variants may belong to one family while remaining separate album releases.
- Representative display metadata: derived display values chosen from evidence, not a blind copy of one Spotify album or track.
- Representative playback metadata: a concrete playable source-track choice for a `release_track` or future `recording_track`; this may differ from the best representative display row.

Current UI direction:
- release-track identity is the preferred same-song identity for liked/display state.
- Activity/Listened display grouping prefers `release_track_id`, then falls back to Spotify track id, then normalized text identity.
- Track preview payloads expose release-track metadata separately from Spotify track id/URI, and track overlays may show release, recording, and family relation evidence without changing playback identity.
- User-facing relation badges concatenate `D/R/V/C`: duplicate source-track grouping, recording group, variation/context family, and cover/remix/rework family.
- Spotify track IDs remain concrete playback and provider-version identifiers.
- Future `recording_track` identity should become the default normal-song aggregation layer only after read-only evidence, review policy, and representative playback selection are defined.
- Current Recording Tracks Identity Audit support is read-only/manual-review only: candidate evidence and saved decisions help inspect quality, but saved reviews do not create or apply `recording_track` identity.
- `recording_track` should expose release appearances and selectable playback source versions without hiding the underlying `release_track` provenance.
- Track Family should not be used for playback substitution without returning a playable representative Spotify URI.
- Related-album UI should keep exact release-track source versions, same-recording appearances, and broader Track Family variations visibly distinct. Recording view may default to the representative release appearance; release view should expose same-release source versions.
- Representative track pages may show Track Family rows for alternates, variations, covers, remixes, and related derived versions. Do not put same-recording release appearances there; those belong with recording/release appearance rows even when they come from different albums.
- Generated recording/track-family candidate tables are evidence caches only. They are allowed to speed up view lookup, but they are not durable identity promotion tables.

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

### Release-Track Boundary

Keep `release_track` tighter than `recording_track`.

Classify as the same `release_track` only when the candidate belongs to the same release context or a near-release metadata cluster:

- same or equivalent album/release context
- same primary track artist
- same base title
- same disc and track position when available
- same or near-identical duration
- no meaningful different-version signal
- release dates are within about one year, or the album IDs/titles clearly represent the same release with casing, punctuation, spacing, availability, or metadata drift

Do not merge later remasters, rereleases, deluxe editions, compilations, soundtrack appearances, or singles into the same `release_track` just because they are the same recording. Those usually remain separate release tracks and become same-`recording_track` candidates when recording evidence supports it.

Remaster wording on the same album should be treated carefully:

- if evidence shows the suffix is metadata drift inside the same release context, it can be same `release_track`
- if it represents a later remaster or reissue context, keep separate `release_track` and group under `recording_track`

### Recording-Track Candidate

Classify separate release tracks as the same future `recording_track` when evidence supports the same core recorded performance or master lineage across releases.

Strong candidate signals:

- same ISRC
- compatible normalized title, primary artist, and release context even when ISRC differs or is missing, especially for remaster, reissue, expanded/deluxe edition, single, compilation, or soundtrack appearances
- same or near-identical duration as a positive corroborating signal
- same primary artist
- compatible title after removing non-identity release/version annotations
- album relationship is original/single/compilation/soundtrack/rerelease/remaster, not a clearly different performance context
- no demo, live, acoustic, remix, alternate-take, rerecording, or materially different edit signal

Potential same-`recording_track` relationships include:

- album release and single release of the same track
- compilation or soundtrack appearance of the same track
- regional rerelease with the same core track
- remaster, reissue, expanded edition, deluxe edition, or anniversary edition where the song structure and performance are the same
- mono/stereo variants when evidence supports the same session or master lineage
- clean/explicit variants when evidence supports the same recording lineage; preserve content-rating metadata for filtering and playback preference

Do not use `recording_track` to hide provenance. A `recording_track` should preserve each release appearance, album context, source Spotify identity, and evidence reason. It may choose a representative release track and a separate concrete playable source track, but Spotify source track id/URI remain the playback identity.

Representative selection should prefer a source-backed original album appearance when available. Use rereleases/remasters, singles, soundtracks, and compilations as progressively weaker fallbacks. Within a Track Family, an explicit derived label such as remix, live, rework, alternate version, or `Again Again Version` must not outrank an available clean base-title recording merely because the derived version has album context. Within one recording, collapse single/EP and album appearances to one recording representative: prefer the first full-album appearance, then use single/EP as fallback when no album appearance exists. Prefer a clean base title over format/remaster suffixes when evidence is otherwise compatible. Compilation appearances should not become the representative only because they have playable source metadata if an original album appearance is also source-backed.

Incremental generated-cluster refresh must match full-rebuild grouping. When exact normalized artist names are split across provider-backed and text-history artist IDs, scoped refresh should include every exact-name identity before rebuilding candidates. This expansion is candidate-cache scope only; it does not merge durable artist identities.

Current debug candidate evidence buckets:

- `same_isrc`
- `conflicting_isrc_but_compatible_metadata`
- `missing_isrc_but_compatible_metadata`
- `partial_isrc_match`
- `variant_flag_excluded`
- `metadata_review_required`

Different ISRC is expected for some remasters and alternate masters. Treat compatible different-ISRC candidates as review-required unless future manual review findings justify a narrower safe path.

Saved recording-track candidate reviews may record `accepted`, `rejected`, `unsure`, `needs_more_metadata`, `wrong_representative`, `maybe_split`, or `maybe_merge_more`, with notes and candidate snapshots. These saved reviews are audit evidence only and must not be interpreted as active canonical identity.

Escalate to Track Family, not same `recording_track`, when the candidate appears to be:

- a live performance
- a demo
- an acoustic or session version
- an alternate take
- an instrumental version
- a remix or rework
- a rerecording
- a live/studio mismatch
- a radio/edit version with meaningful structural or duration changes
- a structural segment such as `part 1`, `part 2`, `pt. 1`, intro, interlude, skit, or reprise
- different named/attributed mix or version labels, unless the exact variant label repeats and strong recording evidence such as same ISRC supports grouping that subgroup

Radio edits are borderline. Treat them as review-required and record the relationship strength rather than auto-collapsing into `recording_track`.

Clean/explicit variants should usually be considered the same `recording_track` when other recording evidence agrees, but the content-rating distinction must remain available to the frontend for filtering and playback preference.

### Explicit, Clean, Video, Demo, Or Remix Variant

Treat these as identity-affecting signals.

Default handling:

- explicit vs clean: usually same recording lineage; preserve a content-rating flag so the frontend can filter or prefer one version
- video version: usually separate release track, same Track Family only if audio/content lineage supports it
- demo: separate release track, usually same Track Family
- instrumental: separate recording/listening object, usually same Track Family
- remix/rework: separate release track, usually same Track Family only when policy allows derived-version grouping
- live/acoustic/session/version/edit/mix labels: use the track-variant policy and escalate when overloaded wording is ambiguous

Classify as same release track only when evidence shows the flag is a metadata annotation and not a materially different listening object.

### Duration Mismatch

Small `duration_ms` differences can be metadata noise. Larger Spotify catalog duration differences are review signals, not automatic split proof, because unavailable tracks, edition drift, and Spotify display/playback disagreement can make stored duration unreliable. Matching durations are useful corroboration, but differing durations do not rule out the same recording or release when normalized title, artist, and release context agree.

Review rules:

- same playback length despite Spotify metadata drift can still be same release track
- one-to-two second differences are weak evidence and should not dominate title/artist/release-context agreement
- larger differences require review, but should not automatically split candidates that share normalized title and compatible artist/release evidence
- same-album duplicate release-track repair may ignore catalog duration differences when title, artist, and release context agree, because duration drift has already been observed in Spotify source rows
- if source mappings are already accepted into the same release track and manual review explains the drift, keep the release join and use a representative duration instead of reopening identity
- representative durations chosen from conflicting catalog values must be marked uncertain; do not treat the longest or shortest source duration as authoritative without stronger playback evidence
- consistent duration differences across many sibling tracks may indicate systematic provider metadata drift
- isolated large duration differences should remain unresolved or separate until explained

When playback evidence is used, record that it came from manual review or a future observed-playback source and should be treated separately from catalog metadata.

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
- automatic `recording_track` creation or representative playback selection
- identity mutation from catalog metadata alone
- identity mutation from saved recording-track candidate reviews alone
