"""
RSS news ingestion script for NFL news sources.

Fetches RSS feeds, parses entries, normalizes timestamps to UTC,
captures diffs for updated articles, and saves raw data to JSONL files for audit trail.
"""
import difflib
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import feedparser
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from src.common.logging_config import setup_logging
from src.common.settings import settings

logger = setup_logging(__name__, settings.log_level)


# NFL RSS Feed Sources
# Note: NFL.com and SI.com have discontinued their RSS feeds
NFL_RSS_FEEDS = [
    {
        "name": "ESPN_NFL",
        "url": "https://www.espn.com/espn/rss/nfl/news",
    },
    {
        "name": "CBS_NFL",
        "url": "https://www.cbssports.com/rss/headlines/nfl/",
    },
    {
        "name": "ProFootballTalk",
        "url": "https://profootballtalk.nbcsports.com/feed/",
    },
    # Additional feeds can be added here as discovered
    # Examples: Yahoo Sports, Bleacher Report, The Athletic, etc.
]


# Cache file for tracking previously seen articles (for diff detection)
ARTICLE_CACHE_FILE = settings.data_ref_dir / "article_cache.json"


def normalize_url(url: str) -> str:
    """
    Normalize a URL to a canonical form for deduplication and diff tracking.
    
    Normalization steps:
    1. Convert domain to lowercase (path is preserved as-is for case-sensitivity)
    2. Remove 'www.' prefix (safe for major news sites)
    3. Prefer HTTPS over HTTP (most modern news sites)
    4. Remove trailing slashes from path
    5. Remove URL fragments (#section)
    6. Remove known tracking query parameters (utm_*, fbclid, etc.)
    7. Sort remaining query parameters for consistency
    8. Remove mobile subdomains (m., mobile.) for major news sites
    
    Safety: If normalization fails for any reason, returns original URL.
    
    Args:
        url: Original URL from RSS feed
        
    Returns:
        Normalized canonical URL string, or original URL if normalization fails
        
    Examples:
        normalize_url("https://www.espn.com/nfl/story/_/id/12345?utm_source=twitter")
        => "https://espn.com/nfl/story/_/id/12345"
        
        normalize_url("http://m.cbssports.com/nfl/news/item?id=789/")
        => "https://cbssports.com/nfl/news/item?id=789"
    """
    if not url:
        return ""
    
    try:
        # Parse URL into components
        parsed = urlparse(url.strip())
        
        # If URL has no scheme or netloc, it's invalid - return as-is
        if not parsed.scheme or not parsed.netloc:
            logger.debug(f"URL missing scheme or netloc, using as-is: {url}")
            return url
        
        # Prefer HTTPS but keep HTTP if that's all they have
        # (RSS feeds from major news sites typically use HTTPS)
        scheme = "https" if parsed.scheme in ("http", "https") else parsed.scheme
        
        # Normalize netloc (domain) - lowercase only
        netloc = parsed.netloc.lower()
        
        # Only remove www/mobile prefixes for known major news domains
        # to avoid breaking sites that require them
        known_news_domains = {
            "espn.com", "cbssports.com", "nfl.com", "nbcsports.com", 
            "si.com", "foxsports.com", "bleacherreport.com"
        }
        
        base_domain = netloc.replace("www.", "").replace("m.", "").replace("mobile.", "")
        
        if base_domain in known_news_domains:
            # Safe to remove prefixes for known sites
            if netloc.startswith("www."):
                netloc = netloc[4:]
            elif netloc.startswith("m."):
                netloc = netloc[2:]
            elif netloc.startswith("mobile."):
                netloc = netloc[7:]
        
        # Normalize path - remove trailing slash but PRESERVE case sensitivity
        # (some servers are case-sensitive)
        path = parsed.path.rstrip("/") if parsed.path else "/"
        if not path:
            path = "/"  # Root path should be /
        
        # Parse and filter query parameters
        # Only remove parameters that are DEFINITELY tracking-related
        # Be conservative - don't remove params that might affect content
        tracking_params = {
            # Google Analytics
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            # Ad platform tracking
            "fbclid", "gclid", "msclkid", "twclid",
            # Social sharing (usually safe to remove)
            "share", "via",
            # Newsletter tracking
            "mc_cid", "mc_eid",
            # Analytics
            "_ga", "_gid", "_hsenc", "_hsmi",
        }
        
        # NOTE: We keep "ref" and "source" as they MIGHT be used for content routing
        # on some sites (though rarely). If we see issues, we can add them back.
        
        query_params = parse_qs(parsed.query, keep_blank_values=False)
        filtered_params = {
            k: v for k, v in query_params.items() 
            if k.lower() not in tracking_params
        }
        
        # Sort query parameters for consistency
        # parse_qs returns lists, so join them
        sorted_params = sorted(filtered_params.items())
        query = urlencode([(k, v[0] if len(v) == 1 else v) for k, v in sorted_params])
        
        # Reconstruct URL (no fragment)
        normalized = urlunparse((
            scheme,
            netloc,
            path,
            parsed.params,  # Keep params (rarely used, but some sites need it)
            query,
            ""  # No fragment
        ))
        
        # Final validation: make sure normalized URL is still valid
        if not normalized or not normalized.startswith(("http://", "https://")):
            logger.warning(f"Normalization produced invalid URL, using original: {url}")
            return url
            
        return normalized
        
    except Exception as e:
        logger.warning(f"Failed to normalize URL '{url}': {e}")
        # ALWAYS return original URL if anything goes wrong
        return url


def clean_html(html_content: str) -> str:
    """
    Remove HTML tags and extra whitespace from content.
    
    Args:
        html_content: Raw HTML string
        
    Returns:
        Cleaned text content
    """
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    
    # Normalize whitespace
    text = " ".join(text.split())
    
    return text


def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a timestamp string to UTC datetime.
    
    Args:
        timestamp_str: Timestamp string in various formats
        
    Returns:
        UTC datetime object or None if parsing fails
    """
    if not timestamp_str:
        return None
    
    try:
        # Parse the timestamp
        dt = date_parser.parse(timestamp_str)
        
        # Convert to UTC if timezone-aware, otherwise assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        
        return dt
    except Exception as e:
        logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
        return None


def load_article_cache() -> dict:
    """
    Load previously seen articles from cache file.
    
    Returns:
        Dictionary mapping article identifiers to their content
    """
    if ARTICLE_CACHE_FILE.exists():
        try:
            with open(ARTICLE_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load article cache: {e}")
            return {}
    return {}


def save_article_cache(cache: dict) -> None:
    """
    Save article cache to disk.
    
    Args:
        cache: Dictionary of article data to cache
    """
    try:
        with open(ARTICLE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save article cache: {e}")


def compute_diff(old_text: str, new_text: str, context_lines: int = 3) -> dict:
    """
    Compute a unified diff between old and new text.
    
    Args:
        old_text: Previous version of the text
        new_text: Current version of the text
        context_lines: Number of context lines to include
        
    Returns:
        Dictionary with diff information and key changes
    """
    # Split into lines for diff
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    
    # Generate unified diff
    diff_lines = list(difflib.unified_diff(
        old_lines, 
        new_lines,
        fromfile='previous',
        tofile='current',
        n=context_lines,
        lineterm=''
    ))
    
    # Extract added/removed content (excluding diff headers)
    added_lines = []
    removed_lines = []
    
    for line in diff_lines:
        if line.startswith('+') and not line.startswith('+++'):
            added_lines.append(line[1:].strip())
        elif line.startswith('-') and not line.startswith('---'):
            removed_lines.append(line[1:].strip())
    
    # Compute change statistics
    total_changes = len(added_lines) + len(removed_lines)
    
    return {
        "has_changes": total_changes > 0,
        "added_lines": added_lines[:10],  # Limit to first 10 for brevity
        "removed_lines": removed_lines[:10],
        "total_changes": total_changes,
        "unified_diff": ''.join(diff_lines) if total_changes > 0 else None,
        "change_summary": f"+{len(added_lines)} -{len(removed_lines)} lines"
    }


def compute_content_hash(source: str, title: str, body: str) -> str:
    """
    Compute a SHA256 hash of the content for deduplication.
    
    Args:
        source: Feed source name
        title: Article title
        body: Article body text
        
    Returns:
        Hex digest of the content hash
    """
    content = f"{source}|{title}|{body}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def fetch_rss_feed(feed_url: str, timeout: int = 10) -> Optional[dict]:
    """
    Fetch RSS feed from URL with timeout.
    
    Args:
        feed_url: URL of the RSS feed
        timeout: Request timeout in seconds
        
    Returns:
        Parsed feed dict or None on error
    """
    try:
        response = httpx.get(feed_url, timeout=timeout, follow_redirects=True)
        response.raise_for_status()
        
        # Parse the RSS feed
        feed = feedparser.parse(response.content)
        return feed
    except Exception as e:
        logger.error(f"Failed to fetch feed from {feed_url}: {e}")
        return None


def parse_feed_entry(
    entry: Any, 
    source_name: str, 
    t_first_seen: datetime,
    article_cache: dict
) -> dict:
    """
    Parse a single RSS feed entry into a normalized dictionary.
    
    Args:
        entry: feedparser entry object
        source_name: Name of the RSS source
        t_first_seen: Timestamp when we first saw this entry
        article_cache: Cache of previously seen articles for diff detection
        
    Returns:
        Normalized entry dictionary
    """
    # Extract basic fields
    url_raw = entry.get("link", "")
    url = normalize_url(url_raw)  # Normalize URL for consistent deduplication
    
    # GUID: prefer feed's ID, but if not available, use normalized URL
    # This ensures consistent GUID even when link has different tracking params
    guid = entry.get("id") or url  # Use normalized URL as fallback!
    
    title = entry.get("title", "")
    
    # Get content (various fields depending on feed format)
    # Feeds use different standards: Atom (summary/content) vs RSS (description)
    # We check all common fields and log which one was used for debugging
    content = ""
    content_source = None
    
    if hasattr(entry, "summary") and entry.summary:
        content = entry.summary
        content_source = "summary"
    elif hasattr(entry, "description") and entry.description:
        content = entry.description
        content_source = "description"
    elif hasattr(entry, "content") and entry.content:
        content = entry.content[0].get("value", "")
        content_source = "content[0]"
    
    if not content:
        logger.warning(f"No content found for entry: {url or 'unknown'}")
    
    # Clean HTML from content
    body_clean = clean_html(content)
    
    # Parse timestamps
    t_published = None
    t_updated = None
    
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        t_published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    elif hasattr(entry, "published"):
        t_published = parse_timestamp(entry.published)
    
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        t_updated = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    elif hasattr(entry, "updated"):
        t_updated = parse_timestamp(entry.updated)
    
    # Determine canonical news timestamp: published → updated → first_seen
    # NOTE: For betting purposes, if t_updated exists and is AFTER t_published,
    # this could be a material update (injury status change, trade completion, etc.)
    # that should be treated as a new edge event, not just a typo fix.
    t_news_utc = t_published or t_updated or t_first_seen
    t_source = "published" if t_published else ("updated" if t_updated else "first_seen")
    
    # Flag if this article has been updated after initial publication
    is_updated = bool(t_updated and t_published and t_updated > t_published)
    
    # Compute content hash for deduplication
    content_hash = compute_content_hash(source_name, title, body_clean)
    
    # Check if we've seen this article before (for diff detection)
    # IMPORTANT: Always use normalized URL for cache key to ensure same article
    # is recognized even with different tracking params, www prefix, etc.
    cache_key = f"{source_name}:{url}"  # url is already normalized
    previous_version = article_cache.get(cache_key)
    
    diff_info = None
    if previous_version and previous_version.get("body") != body_clean:
        # Article content has changed - compute diff
        diff_info = compute_diff(previous_version["body"], body_clean)
        logger.info(
            f"Content change detected: '{title[:50]}...' | {diff_info['change_summary']}"
        )
        
        # Log key changes for betting-relevant updates
        if diff_info["removed_lines"]:
            logger.info(f"  Removed: {diff_info['removed_lines'][:2]}")
        if diff_info["added_lines"]:
            logger.info(f"  Added: {diff_info['added_lines'][:2]}")
    
    # Update cache with current version
    article_cache[cache_key] = {
        "body": body_clean,
        "headline": title,
        "last_seen": t_first_seen.isoformat(),
        "content_hash": content_hash
    }
    
    # Build normalized entry
    normalized_entry = {
        "source": source_name,
        "guid": guid,
        "url": url,  # Normalized URL
        "url_raw": url_raw,  # Keep original for audit trail
        "headline": title,
        "body": body_clean,
        "t_published_utc": t_published.isoformat() if t_published else None,
        "t_updated_utc": t_updated.isoformat() if t_updated else None,
        "t_first_seen_utc": t_first_seen.isoformat(),
        "t_news_utc": t_news_utc.isoformat(),
        "t_source": t_source,
        "is_updated": is_updated,  # Flag for material updates
        "content_hash": content_hash,
        "diff": diff_info,  # Diff from previous version if available
        "raw_entry": {
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "updated": entry.get("updated"),
            "summary": entry.get("summary"),
            "id": entry.get("id"),
        },
    }
    
    # Log if this is a material update (for betting edge detection)
    if is_updated:
        time_diff = (t_updated - t_published).total_seconds() / 60
        logger.info(
            f"Updated article detected: '{title[:60]}...' "
            f"(+{time_diff:.0f} min after publish)"
        )
    
    return normalized_entry


def save_to_jsonl(entries: list[dict], output_dir: Path) -> Path:
    """
    Save entries to a JSONL file organized by date.
    
    Args:
        entries: List of normalized entry dictionaries
        output_dir: Base directory for JSONL files
        
    Returns:
        Path to the created JSONL file
    """
    # Create date-based subdirectory
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_dir = output_dir / today
    date_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"rss_pull_{timestamp}.jsonl"
    filepath = date_dir / filename
    
    # Write entries as JSONL
    with open(filepath, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    
    logger.info(f"Saved {len(entries)} entries to {filepath}")
    return filepath


def pull_all_feeds() -> tuple[list[dict], dict]:
    """
    Pull all configured NFL RSS feeds and return normalized entries in chronological order.
    
    This function ensures entries are processed and returned in chronological order
    (oldest first) based on their published/updated timestamps. This is critical
    for proper odds linking since we need to match news to pre-news baseline odds.
    
    Returns:
        Tuple of (chronologically sorted list of normalized entries, updated article cache)
    """
    all_entries = []
    t_first_seen = datetime.now(timezone.utc)
    
    # Load article cache for diff detection
    article_cache = load_article_cache()
    logger.info(f"Loaded {len(article_cache)} cached articles for diff detection")
    
    for feed_config in NFL_RSS_FEEDS:
        source_name = feed_config["name"]
        feed_url = feed_config["url"]
        
        logger.info(f"Fetching feed: {source_name} from {feed_url}")
        
        feed = fetch_rss_feed(feed_url)
        if not feed:
            logger.warning(f"Skipping {source_name} due to fetch error")
            continue
        
        if not hasattr(feed, "entries") or not feed.entries:
            logger.warning(f"No entries found in feed: {source_name}")
            continue
        
        logger.info(f"Found {len(feed.entries)} entries in {source_name}")
        
        # Sort entries chronologically by published/updated time before processing
        # This ensures we process older news before newer news (critical for odds linking)
        sorted_entries = sorted(
            feed.entries,
            key=lambda e: (
                e.get("published_parsed") or 
                e.get("updated_parsed") or 
                (0, 0, 0, 0, 0, 0)  # Fallback for entries without timestamps
            )
        )
        
        logger.info(f"Sorted {len(sorted_entries)} entries chronologically")
        
        for entry in sorted_entries:
            try:
                normalized_entry = parse_feed_entry(entry, source_name, t_first_seen, article_cache)
                all_entries.append(normalized_entry)
            except Exception as e:
                logger.error(f"Error parsing entry from {source_name}: {e}")
                continue
    
    # Sort all entries chronologically across ALL feeds before returning
    # Use t_news_utc which is our canonical timestamp (published → updated → first_seen)
    all_entries_sorted = sorted(
        all_entries,
        key=lambda e: e.get("t_news_utc", "")  # ISO string sorts correctly
    )
    
    if all_entries_sorted:
        first_time = all_entries_sorted[0].get("t_news_utc", "unknown")
        last_time = all_entries_sorted[-1].get("t_news_utc", "unknown")
        logger.info(
            f"Chronological range: {first_time} → {last_time} "
            f"({len(all_entries_sorted)} entries)"
        )
    
    return all_entries_sorted, article_cache


def main():
    """Main entry point for RSS pull script."""
    logger.info("=" * 80)
    logger.info("Starting RSS news ingestion")
    logger.info("=" * 80)
    
    # Pull all feeds (returns entries and updated cache)
    entries, article_cache = pull_all_feeds()
    
    if not entries:
        logger.warning("No entries collected from any feed")
        return
    
    logger.info(f"Total entries collected: {len(entries)}")
    
    # Save to JSONL
    output_path = save_to_jsonl(entries, settings.data_raw_news_dir)
    
    # Save updated article cache for future diff detection
    save_article_cache(article_cache)
    logger.info(f"Updated article cache with {len(article_cache)} articles")
    
    # Count articles with diffs (material updates)
    articles_with_diffs = sum(1 for e in entries if e.get("diff") and e["diff"]["has_changes"])
    
    # Print summary statistics
    sources = {}
    for entry in entries:
        source = entry["source"]
        sources[source] = sources.get(source, 0) + 1
    
    logger.info("=" * 80)
    logger.info("Summary by source:")
    for source, count in sorted(sources.items()):
        logger.info(f"  {source}: {count} entries")
    if articles_with_diffs > 0:
        logger.info(f"  🔄 {articles_with_diffs} articles with content changes detected")
    logger.info("=" * 80)
    logger.info(f"Output saved to: {output_path}")
    logger.info(f"Article cache saved to: {ARTICLE_CACHE_FILE}")
    logger.info("RSS ingestion complete")


if __name__ == "__main__":
    main()

