# TODO

## Image-based injury reports

- Add a `news_media` PostgreSQL table linked to `news_events` by `news_id`.
- Store the GCS URI, original X URL, MIME type, media type, byte size, SHA-256,
  ingest timestamp, and processing status for each attachment.
- Prevent orphaned media by recording upload failures and reconciling GCS media
  objects whose parent news event was not persisted.
- Run Google Document AI or Cloud Vision OCR on injury-report images, preserving
  extracted text, layout coordinates, confidence, provider, and model version.
- Use Claude to convert OCR/layout output into structured injury claims:
  player, team, report date, practice participation, game status, injury/body
  part, confidence, and exact evidence text.
- Use Claude Vision as a fallback for images whose tables or layout are not
  reliably captured by OCR.
- Resolve extracted player/team mentions only against canonical registry
  candidates and retain unresolved or ambiguous rows for review.
- Link every structured claim to `news_id`, `media_id`, the source image URI,
  and any resulting odds links so the analysis remains auditable.
- Make processing asynchronous and idempotent using the media content hash and
  processing version.
- Add fixtures for team injury tables, handwritten annotations, screenshots,
  low-resolution images, duplicate attachments, multi-page reports, and OCR
  failures.
