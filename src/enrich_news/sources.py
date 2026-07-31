"""Bounded article and media preparation for enrichment.

This module never writes to GCS or PostgreSQL. Video files are temporary and only
representative frames are retained in memory for the model request.
"""

from __future__ import annotations

import hashlib
import io
import ipaddress
import math
import re
import socket
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import imageio_ffmpeg
import requests
from bs4 import BeautifulSoup
from PIL import Image, ImageOps

from src.enrich_news.models import NewsRecord

ARTICLE_MAX_BYTES = 3 * 1024 * 1024
ARTICLE_MAX_CHARACTERS = 12_000
IMAGE_MAX_BYTES = 10 * 1024 * 1024
VIDEO_MAX_BYTES = 100 * 1024 * 1024
MAX_REDIRECTS = 4
MAX_ARTICLES = 3
MAX_VIDEO_FRAMES = 8
MAX_VIDEO_SECONDS = 300.0
REQUEST_TIMEOUT = (5, 20)
USER_AGENT = "AI-SportsBettor-Enrichment/1.0 (+research; contact=operator)"

_DURATION_PATTERN = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")
_ARTICLE_CONTENT_TYPES = {"text/html", "application/xhtml+xml", "text/plain"}
_IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_EXCLUDED_ARTICLE_HOSTS = {
    "x.com",
    "www.x.com",
    "twitter.com",
    "www.twitter.com",
    "pic.twitter.com",
}


@dataclass(frozen=True)
class ImagePayload:
    source_ref: str
    media_type: str
    data: bytes
    sha256: str


@dataclass
class CollectedEvidence:
    text_sections: list[str] = field(default_factory=list)
    images: list[ImagePayload] = field(default_factory=list)
    manifest: dict[str, Any] = field(default_factory=lambda: {"articles": [], "media": []})
    warnings: list[str] = field(default_factory=list)

    def as_prompt_text(self) -> str:
        return "\n\n".join(self.text_sections)

    def source_refs(self) -> list[str]:
        """Return the canonical evidence identifiers exposed to the provider."""

        refs = [
            "tweet",
            *(
                item["source_ref"]
                for item in self.manifest["articles"]
                if item.get("source_ref") and item.get("status") in {"fetched", "prepared_fixture"}
            ),
            *(item["source_ref"] for item in self.manifest["media"] if item.get("source_ref")),
            *(image.source_ref for image in self.images),
        ]
        return list(dict.fromkeys(refs))


@dataclass(frozen=True)
class DownloadedResource:
    final_url: str
    content_type: str
    body: bytes


def _is_public_ip(value: str) -> bool:
    address = ipaddress.ip_address(value)
    return not (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def validate_public_url(
    url: str,
    *,
    resolver: Callable[..., list[tuple[Any, ...]]] = socket.getaddrinfo,
) -> str:
    """Reject local/private network targets before each HTTP request."""

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("URL scheme must be http or https")
    if parsed.username or parsed.password:
        raise ValueError("URLs containing credentials are not allowed")
    if not parsed.hostname:
        raise ValueError("URL hostname is required")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ValueError("URL port is invalid") from exc
    if port not in {80, 443}:
        raise ValueError("only standard HTTP and HTTPS ports are allowed")

    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise ValueError("local hostnames are not allowed")
    try:
        if not _is_public_ip(hostname):
            raise ValueError("private or reserved IP targets are not allowed")
    except ValueError as exc:
        if "does not appear to be" not in str(exc):
            raise
        addresses = {
            item[4][0]
            for item in resolver(hostname, port, type=socket.SOCK_STREAM)
            if item and len(item) >= 5 and item[4]
        }
        if not addresses or any(not _is_public_ip(address) for address in addresses):
            raise ValueError("hostname resolves to a private or reserved address") from None
    return url


def download_public_resource(
    url: str,
    *,
    allowed_content_types: set[str],
    max_bytes: int,
    session: requests.Session | None = None,
) -> DownloadedResource:
    """Download a bounded public resource while validating every redirect."""

    active_session = session or requests.Session()
    current_url = url
    for _redirect in range(MAX_REDIRECTS + 1):
        validate_public_url(current_url)
        response = active_session.get(
            current_url,
            allow_redirects=False,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            stream=True,
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code in _REDIRECT_STATUSES:
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise ValueError("redirect response did not include Location")
            current_url = urljoin(current_url, location)
            continue
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
        if content_type not in allowed_content_types:
            response.close()
            raise ValueError(f"unsupported content type: {content_type or 'missing'}")
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            response.close()
            raise ValueError(f"resource exceeds {max_bytes} byte limit")
        chunks: list[bytes] = []
        byte_count = 0
        try:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                byte_count += len(chunk)
                if byte_count > max_bytes:
                    raise ValueError(f"resource exceeds {max_bytes} byte limit")
                chunks.append(chunk)
        finally:
            response.close()
        return DownloadedResource(
            final_url=current_url,
            content_type=content_type,
            body=b"".join(chunks),
        )
    raise ValueError(f"resource exceeded {MAX_REDIRECTS} redirects")


def extract_article_text(resource: DownloadedResource) -> tuple[str | None, str]:
    charset_match = re.search(
        rb"<meta[^>]+charset=[\"']?([A-Za-z0-9._-]+)",
        resource.body[:16_384],
        flags=re.IGNORECASE,
    )
    charset = charset_match.group(1).decode("ascii", errors="ignore") if charset_match else "utf-8"
    decoded = resource.body.decode(charset or "utf-8", errors="replace")
    if resource.content_type == "text/plain":
        text = " ".join(decoded.split())
        return None, text[:ARTICLE_MAX_CHARACTERS]

    soup = BeautifulSoup(decoded, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else None
    for element in soup(
        ["script", "style", "noscript", "nav", "footer", "header", "aside", "form"]
    ):
        element.decompose()
    preferred = soup.find("article") or soup.find("main") or soup.body or soup
    paragraphs = [
        node.get_text(" ", strip=True) for node in preferred.find_all(["p", "h1", "h2", "h3", "li"])
    ]
    text = "\n".join(value for value in paragraphs if value)
    if not text:
        text = preferred.get_text(" ", strip=True)
    return title, text[:ARTICLE_MAX_CHARACTERS]


def article_urls(source_entities: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for entity in source_entities.get("urls", []) or []:
        if not isinstance(entity, dict):
            continue
        candidate = entity.get("unwound_url") or entity.get("expanded_url") or entity.get("url")
        if not candidate or not isinstance(candidate, str):
            continue
        hostname = (urlparse(candidate).hostname or "").lower()
        if hostname in _EXCLUDED_ARTICLE_HOSTS:
            continue
        if candidate not in urls:
            urls.append(candidate)
    return urls[:MAX_ARTICLES]


def _normalize_image(body: bytes, source_ref: str) -> ImagePayload:
    if len(body) > IMAGE_MAX_BYTES:
        raise ValueError(f"image exceeds {IMAGE_MAX_BYTES} byte limit")
    with Image.open(io.BytesIO(body)) as image:
        image = ImageOps.exif_transpose(image)
        if getattr(image, "is_animated", False):
            image.seek(0)
        image = image.convert("RGB")
        image.thumbnail((1568, 1568))
        output = io.BytesIO()
        image.save(output, format="JPEG", quality=88, optimize=True)
    normalized = output.getvalue()
    return ImagePayload(
        source_ref=source_ref,
        media_type="image/jpeg",
        data=normalized,
        sha256=hashlib.sha256(normalized).hexdigest(),
    )


def _ffmpeg_path() -> str:
    return imageio_ffmpeg.get_ffmpeg_exe()


def _video_duration(video_path: Path) -> float | None:
    result = subprocess.run(
        [_ffmpeg_path(), "-hide_banner", "-i", str(video_path)],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    match = _DURATION_PATTERN.search(result.stderr)
    if not match:
        return None
    hours, minutes, seconds = match.groups()
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def _video_sample_timestamps(duration: float) -> list[float]:
    bounded_duration = min(max(duration, 0.0), MAX_VIDEO_SECONDS)
    frame_count = min(MAX_VIDEO_FRAMES, max(1, math.ceil(bounded_duration / 30)))
    if frame_count == 1:
        return [0.0]
    sample_end = max(0.0, bounded_duration - min(0.5, bounded_duration / frame_count))
    return [index * sample_end / (frame_count - 1) for index in range(frame_count)]


def extract_video_frames(video_path: Path, source_ref: str) -> list[ImagePayload]:
    duration = _video_duration(video_path) or 60.0
    timestamps = _video_sample_timestamps(duration)
    frames: list[ImagePayload] = []
    with tempfile.TemporaryDirectory(prefix="sports_video_frames_") as frame_dir:
        for index, timestamp in enumerate(timestamps):
            output_path = Path(frame_dir) / f"frame_{index:03d}.jpg"
            try:
                subprocess.run(
                    [
                        _ffmpeg_path(),
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-ss",
                        f"{timestamp:.3f}",
                        "-i",
                        str(video_path),
                        "-frames:v",
                        "1",
                        "-vf",
                        "scale='min(1568,iw)':-2",
                        "-q:v",
                        "3",
                        "-y",
                        str(output_path),
                    ],
                    capture_output=True,
                    check=True,
                    timeout=60,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                continue
            frames.append(
                _normalize_image(
                    output_path.read_bytes(),
                    f"{source_ref}:frame:{index}",
                )
            )
    if not frames:
        raise ValueError("video contained no extractable frames")
    return frames


def _read_local_file(path_value: str, max_bytes: int) -> bytes:
    path = Path(path_value).expanduser().resolve()
    size = path.stat().st_size
    if size > max_bytes:
        raise ValueError(f"local file exceeds {max_bytes} byte limit")
    return path.read_bytes()


def _collect_article_sources(
    record: NewsRecord,
    evidence: CollectedEvidence,
    *,
    allow_network: bool,
    session: requests.Session,
) -> None:
    for prepared in record.prepared_article_evidence:
        evidence.text_sections.append(
            f"[{prepared.source_ref}] Article title: {prepared.title or 'unknown'}\n"
            f"Article URL: {prepared.url}\nArticle text: {prepared.text}"
        )
        evidence.manifest["articles"].append(
            {
                "source_ref": prepared.source_ref,
                "url": prepared.url,
                "status": "prepared_fixture",
                "characters": len(prepared.text),
            }
        )

    prepared_urls = {item.url for item in record.prepared_article_evidence}
    for index, url in enumerate(article_urls(record.source_entities), start=1):
        if url in prepared_urls:
            continue
        source_ref = f"article:{index}"
        if not allow_network:
            evidence.warnings.append(f"{source_ref}: network disabled; article not fetched")
            evidence.manifest["articles"].append(
                {"source_ref": source_ref, "url": url, "status": "network_disabled"}
            )
            continue
        try:
            resource = download_public_resource(
                url,
                allowed_content_types=_ARTICLE_CONTENT_TYPES,
                max_bytes=ARTICLE_MAX_BYTES,
                session=session,
            )
            title, text = extract_article_text(resource)
            if not text.strip():
                raise ValueError("article contained no extractable text")
            evidence.text_sections.append(
                f"[{source_ref}] Article title: {title or 'unknown'}\n"
                f"Article URL: {resource.final_url}\nArticle text: {text}"
            )
            evidence.manifest["articles"].append(
                {
                    "source_ref": source_ref,
                    "url": url,
                    "final_url": resource.final_url,
                    "status": "fetched",
                    "content_sha256": hashlib.sha256(resource.body).hexdigest(),
                    "characters": len(text),
                }
            )
        except Exception as exc:
            evidence.warnings.append(f"{source_ref}: article unavailable ({type(exc).__name__})")
            evidence.manifest["articles"].append(
                {
                    "source_ref": source_ref,
                    "url": url,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                }
            )


def _collect_media_sources(
    record: NewsRecord,
    evidence: CollectedEvidence,
    *,
    allow_network: bool,
    session: requests.Session,
) -> None:
    for prepared in record.prepared_media_evidence:
        details = [
            f"[{prepared.source_ref}] Prepared {prepared.media_type} evidence.",
        ]
        if prepared.description:
            details.append(f"Visual description: {prepared.description}")
        if prepared.ocr_text:
            details.append(f"Visible text/OCR: {prepared.ocr_text}")
        if prepared.transcript:
            details.append(f"Transcript: {prepared.transcript}")
        evidence.text_sections.append("\n".join(details))
        evidence.manifest["media"].append(
            {
                "source_ref": prepared.source_ref,
                "media_type": prepared.media_type,
                "status": "prepared_fixture",
            }
        )

    for media in record.media:
        source_ref = f"media:{media.media_key}"
        details = [f"[{source_ref}] Attached media type: {media.media_type or 'unknown'}."]
        if media.alt_text:
            details.append(f"Source alt text: {media.alt_text}")
        if media.transcript:
            details.append(f"Provided transcript: {media.transcript}")
        evidence.text_sections.append("\n".join(details))

        media_type = (media.media_type or "").lower()
        is_video = media_type in {"video", "animated_gif"}
        source_url = (
            media.source_url
            if is_video
            else (media.selected_source_url or media.source_url or media.preview_image_url)
        )
        manifest_item: dict[str, Any] = {
            "source_ref": source_ref,
            "media_key": media.media_key,
            "media_type": media_type or None,
        }
        try:
            if media.local_path:
                body = _read_local_file(
                    media.local_path,
                    VIDEO_MAX_BYTES if is_video else IMAGE_MAX_BYTES,
                )
                manifest_item["input"] = "local_file"
            elif allow_network and source_url:
                resource = download_public_resource(
                    source_url,
                    allowed_content_types=(
                        {"video/mp4", "video/quicktime", "application/octet-stream"}
                        if is_video
                        else _IMAGE_CONTENT_TYPES
                    ),
                    max_bytes=VIDEO_MAX_BYTES if is_video else IMAGE_MAX_BYTES,
                    session=session,
                )
                body = resource.body
                manifest_item["input"] = "network"
                manifest_item["final_url"] = resource.final_url
            elif source_url:
                manifest_item["status"] = "network_disabled"
                evidence.warnings.append(f"{source_ref}: network disabled; media not fetched")
                evidence.manifest["media"].append(manifest_item)
                continue
            else:
                manifest_item["status"] = "not_available"
                evidence.warnings.append(f"{source_ref}: no usable media source")
                evidence.manifest["media"].append(manifest_item)
                continue

            if is_video:
                suffix = ".mp4"
                with tempfile.NamedTemporaryFile(
                    prefix="sports_video_",
                    suffix=suffix,
                    delete=False,
                ) as temporary_video:
                    temporary_video.write(body)
                    temporary_video.flush()
                    temporary_video_path = Path(temporary_video.name)
                try:
                    frames = extract_video_frames(temporary_video_path, source_ref)
                finally:
                    temporary_video_path.unlink(missing_ok=True)
                evidence.images.extend(frames)
                manifest_item["status"] = "frames_extracted"
                manifest_item["frame_count"] = len(frames)
                manifest_item["video_sha256"] = hashlib.sha256(body).hexdigest()
                manifest_item["audio_status"] = (
                    "provided_transcript" if media.transcript else "not_transcribed"
                )
            else:
                image = _normalize_image(body, source_ref)
                evidence.images.append(image)
                manifest_item["status"] = "image_prepared"
                manifest_item["content_sha256"] = image.sha256
        except Exception as exc:
            manifest_item["status"] = "failed"
            manifest_item["error_type"] = type(exc).__name__
            evidence.warnings.append(f"{source_ref}: media unavailable ({type(exc).__name__})")
        evidence.manifest["media"].append(manifest_item)


def collect_evidence(
    record: NewsRecord,
    *,
    allow_network: bool = False,
    session: requests.Session | None = None,
) -> CollectedEvidence:
    """Prepare all usable evidence without any durable external writes."""

    evidence = CollectedEvidence()
    evidence.text_sections.append(
        f"[tweet] Author: @{record.author_username or 'unknown'}\n"
        f"Published: {record.published_at or 'unknown'}\n"
        f"Source URL: {record.source_url or 'unknown'}\n"
        f"Tweet text: {record.text}"
    )
    active_session = session or requests.Session()
    _collect_article_sources(
        record,
        evidence,
        allow_network=allow_network,
        session=active_session,
    )
    _collect_media_sources(
        record,
        evidence,
        allow_network=allow_network,
        session=active_session,
    )
    return evidence
