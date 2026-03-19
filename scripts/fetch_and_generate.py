#!/usr/bin/env python3
"""Fetch Gmail newsletter emails and generate RSS feeds.

Reads config.json to determine which senders to fetch, uses
state/feeds.json to track already-processed message IDs (so only
new messages are fetched on each run), and writes RSS 2.0 XML
files to the feeds/ directory.
"""

import base64
import json
import os
import re
import sys
from datetime import datetime, timezone
from email import message_from_bytes
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime
from html import escape
from pathlib import Path

import trafilatura
from trafilatura.metadata import extract_metadata as trafilatura_metadata

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


# ---------------------------------------------------------------------------
# Gmail authentication
# ---------------------------------------------------------------------------

def build_gmail_service():
    """Build and return an authenticated Gmail API service."""
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GMAIL_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GMAIL_CLIENT_ID"],
        client_secret=os.environ["GMAIL_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/gmail.modify"],
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

STATE_FILE = Path(__file__).parent.parent / "state" / "feeds.json"


def load_state():
    """Load persistent per-feed state from disk, or return empty state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    """Write updated state to disk."""
    STATE_FILE.parent.mkdir(exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Gmail fetching
# ---------------------------------------------------------------------------

def mark_message_processed(service, msg_id, mark_read=True, archive=False):
    """Remove UNREAD and/or INBOX labels from a message."""
    labels_to_remove = []
    if mark_read:
        labels_to_remove.append("UNREAD")
    if archive:
        labels_to_remove.append("INBOX")
    if labels_to_remove:
        service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={"removeLabelIds": labels_to_remove},
        ).execute()


def fetch_new_message_ids(service, sender, seen_ids, max_new=50):
    """Return Gmail message IDs from `sender` not already in `seen_ids`.

    Pages through results newest-first and stops as soon as a full page
    consists entirely of already-seen messages, so old history is never
    re-walked after the initial run.
    """
    seen_set = set(seen_ids)
    new_ids = []
    page_token = None

    while len(new_ids) < max_new:
        kwargs = {
            "userId": "me",
            "q": f"from:{sender}",
            "maxResults": 100,
        }
        if page_token:
            kwargs["pageToken"] = page_token

        result = service.users().messages().list(**kwargs).execute()
        messages = result.get("messages", [])
        if not messages:
            break

        page_had_new = False
        for msg in messages:
            if msg["id"] not in seen_set:
                new_ids.append(msg["id"])
                page_had_new = True

        page_token = result.get("nextPageToken")
        # Stop paginating once we hit a page with no new messages
        if not page_token or not page_had_new:
            break

    return new_ids[:max_new]


def fetch_raw_message(service, msg_id):
    """Fetch a single message in raw format and return a parsed email object."""
    result = service.users().messages().get(
        userId="me", id=msg_id, format="raw"
    ).execute()
    # Gmail API returns base64url without padding — add it before decoding
    raw_b64 = result["raw"]
    raw_b64 += "=" * (4 - len(raw_b64) % 4)
    raw = base64.urlsafe_b64decode(raw_b64)
    return message_from_bytes(raw)


# ---------------------------------------------------------------------------
# Email parsing
# ---------------------------------------------------------------------------

def decode_header_value(value):
    """Decode an email header value that may be RFC 2047-encoded."""
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def extract_plain_text(msg):
    """Walk a MIME message and return the first text/plain payload."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode("utf-8", errors="replace")
    else:
        if msg.get_content_type() == "text/plain":
            payload = msg.get_payload(decode=True)
            if payload:
                return payload.decode("utf-8", errors="replace")
    return ""


def extract_url_from_list_post(header_value):
    """Extract the URL from a List-Post header like '<https://example.com/p/slug>'."""
    if not header_value:
        return ""
    match = re.search(r"<(https?://[^>]+)>", header_value)
    return match.group(1) if match else ""


def decode_substack_redirect(redirect_url):
    """Decode a Substack redirect URL to recover the canonical article URL.

    Substack redirect URLs look like:
      https://substack.com/redirect/2/<base64url_payload>.<signature>
    The payload is a JSON object where the 'e' key holds the real URL.
    """
    match = re.search(r"substack\.com/redirect/\d+/([A-Za-z0-9_-]+)", redirect_url)
    if not match:
        return ""
    token = match.group(1).split(".")[0]  # drop signature if present
    token += "=" * (4 - len(token) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(token).decode("utf-8"))
        return payload.get("e", "")
    except Exception:
        return ""


def extract_article_url(msg, plain_text):
    """Try multiple sources to find the canonical article URL.

    Priority:
    1. List-Post email header (cleanest; may be stripped by some clients)
    2. 'View this post on the web at' line in plain text body
    3. First Substack redirect URL decoded from the plain text body
    4. Mailchimp archive URL (mailchi.mp) for non-Substack newsletters
    """
    url = extract_url_from_list_post(msg.get("List-Post", ""))
    if url:
        return url

    match = re.search(
        r"view this post on the web at\s+(https?://\S+)", plain_text, re.IGNORECASE
    )
    if match:
        return match.group(1).rstrip("?")

    match = re.search(r"https?://substack\.com/redirect/\S+", plain_text)
    if match:
        url = decode_substack_redirect(match.group(0))
        if url:
            return url

    match = re.search(r"https://mailchi\.mp/[^\s?]+", plain_text)
    if match:
        return match.group(0)

    return ""


def extract_post_type(msg):
    """Return the post_type from the X-Mailgun-Variables header (e.g. 'podcast', 'newsletter')."""
    variables_str = msg.get("X-Mailgun-Variables", "")
    if not variables_str:
        return ""
    try:
        return json.loads(variables_str).get("post_type", "")
    except Exception:
        return ""


def extract_author_name(from_header):
    """Return just the display name from a From header, stripping the email address."""
    match = re.match(r"^([^<]+)", from_header)
    return match.group(1).strip() if match else from_header


# ---------------------------------------------------------------------------
# Content cleaning
# ---------------------------------------------------------------------------

# Invisible Unicode chars used as email pre-header padding
_INVISIBLE = re.compile(
    r"[\u00ad\u034f\u200b\u200c\u200d\u2060\u2061\u2062\u2063\ufeff\u00a0]+"
)


def strip_invisible_chars(text):
    """Remove invisible Unicode padding characters."""
    return _INVISIBLE.sub("", text)


def is_preheader_paragraph(para):
    """Return True if this paragraph is Substack pre-header boilerplate.

    Pre-headers are typically: a short teaser sentence followed by hundreds
    of invisible padding characters, sometimes ending with a 'View in browser'
    link. After stripping invisible chars they collapse to near-nothing or
    only a short sentence + URL.
    """
    cleaned = strip_invisible_chars(para).strip()
    if len(cleaned) < 120:
        return True
    if re.search(r"view in browser", cleaned, re.IGNORECASE):
        return True
    return False


def strip_header_footer(body):
    """Remove Substack boilerplate from the top and bottom of the plain text."""
    lines = body.split("\n")

    # Drop the "View this post on the web at ..." opening line
    if lines and lines[0].strip().lower().startswith("view this post"):
        lines = lines[1:]

    # Drop leading blank lines
    while lines and not lines[0].strip():
        lines = lines[1:]

    text = "\n".join(lines).strip()

    # Drop everything from the "Unsubscribe" line onward
    unsubscribe_match = re.search(r"\nUnsubscribe https?://\S+", text, re.IGNORECASE)
    if unsubscribe_match:
        text = text[: unsubscribe_match.start()].strip()

    # Drop leading paragraphs that are Substack pre-header padding
    paragraphs = re.split(r"\n\n+", text)
    while paragraphs and is_preheader_paragraph(paragraphs[0]):
        paragraphs = paragraphs[1:]
    text = "\n\n".join(paragraphs).strip()

    return text


def strip_newsletter_intro(text, patterns):
    """Strip leading lines whose text contains any of the given strings.

    Walks line-by-line (skipping blank lines) so that a newsletter using
    single-newline paragraph separators doesn't accidentally match a pattern
    in the middle of the article and delete all remaining content.
    Stripping stops the moment a non-empty line fails to match.
    """
    if not patterns:
        return text
    lines = text.split("\n")
    while lines:
        stripped = lines[0].strip()
        if not stripped:
            lines.pop(0)
            continue
        if any(p.lower() in stripped.lower() for p in patterns):
            lines.pop(0)
        else:
            break
    return "\n".join(lines).strip()


def text_to_html(text):
    """Convert cleaned plain text to simple HTML suitable for an RSS reader.

    Rules:
    - Double-newline boundaries become paragraph breaks.
    - A single-line paragraph that is short and ends without sentence-ending
      punctuation is treated as a section heading (<h3>).
    - Inline Substack redirect links [ https://... ] are stripped.
    """
    text = re.sub(r"\s*\[\s*https?://\S+?\s*\]", "", text)
    text = strip_invisible_chars(text)

    # Split on any newline boundary (handles both single-\n newsletters like
    # Zeteo and double-\n newsletters like Substack — empty lines are filtered).
    raw_lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    html_parts = []

    for para in raw_lines:
        para = strip_invisible_chars(para).strip()
        if not para:
            continue

        joined = para

        if re.match(r"^https?://\S+$", joined):
            continue

        is_heading = (
            len(joined) < 80
            and joined[-1] not in ".!?,;:)\"'"
            and not joined.lower().startswith("http")
        )

        if is_heading:
            html_parts.append(f"<h3>{escape(joined)}</h3>")
        else:
            html_parts.append(f"<p>{escape(joined)}</p>")

    return "\n".join(html_parts)


def make_description(html_content, max_len=300):
    """Extract a plain-text excerpt from HTML content for the <description> field."""
    text = re.sub(r"<[^>]+>", " ", html_content)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        text = text[:max_len].rsplit(" ", 1)[0] + "…"
    return text


# ---------------------------------------------------------------------------
# RSS generation
# ---------------------------------------------------------------------------

def format_rfc2822(dt):
    """Format a datetime as an RFC 2822 date string for RSS."""
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")


def generate_rss_xml(feed_config, items, repo_base_url):
    """Return a complete RSS 2.0 XML string for a feed."""
    feed_url = f"{repo_base_url}/feeds/{feed_config['hash']}.xml"
    now_rfc = format_rfc2822(datetime.now(timezone.utc))

    sorted_items = sorted(items, key=lambda x: x["pub_date"], reverse=True)
    sorted_items = sorted_items[: feed_config["max_items"]]

    item_blocks = []
    for item in sorted_items:
        is_permalink = item["url"].startswith("http")
        item_blocks.append(
            f"""    <item>
      <title>{escape(item['title'])}</title>
      <link>{escape(item['url'])}</link>
      <description>{escape(item['description'])}</description>
      <pubDate>{format_rfc2822(item['pub_date'])}</pubDate>
      <author>{escape(item['author'])}</author>
      <guid isPermaLink="{'true' if is_permalink else 'false'}">{escape(item['guid'])}</guid>
      <content:encoded><![CDATA[{item['content']}]]></content:encoded>
    </item>"""
        )

    items_xml = "\n".join(item_blocks)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:content="http://purl.org/rss/1.0/modules/content/"
  xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>{escape(feed_config['name'])}</title>
    <link>{escape(feed_config['site_url'])}</link>
    <description>{escape(feed_config['description'])}</description>
    <language>en-us</language>
    <lastBuildDate>{now_rfc}</lastBuildDate>
    <atom:link href="{escape(feed_url)}" rel="self" type="application/rss+xml"/>
{items_xml}
  </channel>
</rss>"""


# ---------------------------------------------------------------------------
# Per-feed processing
# ---------------------------------------------------------------------------

def parse_message(service, msg_id, feed_cfg):
    """Fetch and parse one Gmail message into an item dict."""
    msg = fetch_raw_message(service, msg_id)

    subject = decode_header_value(msg.get("Subject", "(no subject)"))
    date_str = msg.get("Date", "")
    from_str = decode_header_value(msg.get("From", ""))

    try:
        pub_date = parsedate_to_datetime(date_str)
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)
    except Exception:
        pub_date = datetime.now(timezone.utc)

    author = extract_author_name(from_str)
    plain_text = extract_plain_text(msg)
    url = extract_article_url(msg, plain_text)
    post_type = extract_post_type(msg)
    clean_text = strip_header_footer(plain_text)
    clean_text = strip_newsletter_intro(
        clean_text, feed_cfg.get("strip_intro_containing", [])
    )
    html_content = text_to_html(clean_text)
    description = make_description(html_content)
    guid = url if url else f"gmail:{msg.get('Message-Id', msg_id)}"

    return {
        "gmail_id": msg_id,
        "title": subject,
        "url": url,
        "pub_date": pub_date.isoformat(),
        "author": author,
        "post_type": post_type,
        "content": html_content,
        "description": description,
        "guid": guid,
    }


def process_feed(service, feed_cfg, feeds_dir, repo_base_url, state, mark_read=False, archive=False):
    """Fetch new emails for one feed, merge with history, write RSS XML.

    Only messages not already recorded in `state` are fetched from Gmail.
    Supports both a single "sender" string and a "senders" array.
    Emails whose subject matches any entry in "skip_if_subject_contains" are dropped.
    """
    name = feed_cfg["name"]
    senders = feed_cfg.get("senders") or [feed_cfg["sender"]]
    skip_subjects = [s.lower() for s in feed_cfg.get("skip_if_subject_contains", [])]
    skip_post_types = [t.lower() for t in feed_cfg.get("skip_if_post_type", [])]

    print(f"\n{'='*60}")
    print(f"Feed: {name}  (senders: {', '.join(senders)})")

    feed_state = state.get(name, {"seen_ids": [], "items": []})
    seen_ids = feed_state.get("seen_ids", [])
    existing_items = feed_state.get("items", [])

    # Collect new IDs across all senders
    all_new_ids = []
    for sender in senders:
        new_ids = fetch_new_message_ids(service, sender, seen_ids, feed_cfg["max_items"])
        all_new_ids.extend(new_ids)
    print(f"New messages: {len(all_new_ids)}  |  Previously seen: {len(seen_ids)}")

    # Fetch and parse only the new ones, applying subject filters
    new_items = []
    for i, msg_id in enumerate(all_new_ids):
        print(f"  Parsing {i + 1}/{len(all_new_ids)}...", end="\r")
        try:
            item = parse_message(service, msg_id, feed_cfg)
            if skip_subjects and any(s in item["title"].lower() for s in skip_subjects):
                print(f"\n  Skipping (subject filter): {item['title']}")
                continue
            if skip_post_types and item.get("post_type", "").lower() in skip_post_types:
                print(f"\n  Skipping (post_type={item['post_type']}): {item['title']}")
                continue
            new_items.append(item)
            if mark_read or archive:
                try:
                    mark_message_processed(service, msg_id, mark_read, archive)
                except Exception as e:
                    print(f"\n  Warning: could not mark message {msg_id}: {e}")
        except Exception as e:
            print(f"\n  Warning: could not parse message {msg_id}: {e}")

    if new_ids:
        print(f"\nParsed {len(new_items)} new items")
    else:
        print("No new messages — feed unchanged")

    # Merge new + existing, sort newest-first, cap at max_items
    all_items = new_items + existing_items
    all_items.sort(key=lambda x: x["pub_date"], reverse=True)
    all_items = all_items[: feed_cfg["max_items"]]

    # Convert ISO date strings back to datetime objects for RSS formatting
    rss_items = []
    for item in all_items:
        rss_item = dict(item)
        if isinstance(rss_item["pub_date"], str):
            rss_item["pub_date"] = datetime.fromisoformat(rss_item["pub_date"])
        rss_items.append(rss_item)

    rss_xml = generate_rss_xml(feed_cfg, rss_items, repo_base_url)
    output_path = feeds_dir / f"{feed_cfg['hash']}.xml"
    output_path.write_text(rss_xml, encoding="utf-8")
    print(f"Written → {output_path.name}")

    # Persist updated state: union of all seen IDs + trimmed item list
    all_seen = list(set(seen_ids) | set(all_new_ids))
    state[name] = {"seen_ids": all_seen, "items": all_items}


# ---------------------------------------------------------------------------
# Read Later helpers
# ---------------------------------------------------------------------------

def fetch_article(url):
    """Fetch a URL and return (title, author, html_content) via trafilatura.

    Returns (None, None, None) if the page cannot be fetched or parsed.
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None, None, None

        html_content = trafilatura.extract(
            downloaded,
            output_format="html",
            include_formatting=True,
            include_links=False,
            no_fallback=False,
        )

        title = None
        author = None
        try:
            meta = trafilatura_metadata(downloaded)
            if meta:
                title = meta.title or None
                author = meta.author or None
        except Exception:
            pass

        return title, author, html_content
    except Exception as e:
        print(f"  Warning: could not fetch article {url}: {e}")
        return None, None, None


def process_read_later_feed(service, feed_cfg, feeds_dir, repo_base_url, state,
                            mark_read=False, archive=False):
    """Process a read-later feed: emails contain URLs to fetch and save as RSS items."""
    name = feed_cfg["name"]
    senders = feed_cfg.get("senders") or [feed_cfg["sender"]]

    print(f"\n{'='*60}")
    print(f"Feed: {name}  (read-later, senders: {', '.join(senders)})")

    feed_state = state.get(name, {"seen_ids": [], "items": []})
    seen_ids = feed_state.get("seen_ids", [])
    existing_items = feed_state.get("items", [])

    all_new_ids = []
    for sender in senders:
        new_ids = fetch_new_message_ids(service, sender, seen_ids, feed_cfg["max_items"])
        all_new_ids.extend(new_ids)
    print(f"New messages: {len(all_new_ids)}  |  Previously seen: {len(seen_ids)}")

    new_items = []
    for i, msg_id in enumerate(all_new_ids):
        print(f"  Processing {i + 1}/{len(all_new_ids)}...", end="\r")
        try:
            msg = fetch_raw_message(service, msg_id)
            subject = decode_header_value(msg.get("Subject", ""))
            date_str = msg.get("Date", "")

            try:
                pub_date = parsedate_to_datetime(date_str)
                if pub_date.tzinfo is None:
                    pub_date = pub_date.replace(tzinfo=timezone.utc)
            except Exception:
                pub_date = datetime.now(timezone.utc)

            plain_text = extract_plain_text(msg)

            # Find URL — body first, then subject
            url_match = re.search(r"https?://\S+", plain_text)
            if not url_match:
                url_match = re.search(r"https?://\S+", subject)
            if not url_match:
                print(f"\n  Skipping (no URL found): {subject}")
                continue

            article_url = url_match.group(0).rstrip("?.,;)")

            # Use email subject as title unless it looks like a bare URL
            if subject and not re.match(r"^https?://", subject.strip()):
                title = subject
            else:
                title = None  # will use trafilatura's extracted title

            print(f"\n  Fetching: {article_url}")
            fetched_title, author, html_content = fetch_article(article_url)

            if not title:
                title = fetched_title or article_url

            # Any non-URL text in the body becomes a user annotation shown first
            annotation = re.sub(r"https?://\S+", "", plain_text).strip()
            annotation = re.sub(r"\s+", " ", annotation).strip()
            if annotation:
                html_content = f"<p><em>{escape(annotation)}</em></p>\n" + (html_content or "")

            if not html_content:
                html_content = f'<p><a href="{escape(article_url)}">{escape(article_url)}</a></p>'

            description = make_description(html_content)

            item = {
                "gmail_id": msg_id,
                "title": title,
                "url": article_url,
                "pub_date": pub_date.isoformat(),
                "author": author or "Scott Angstreich",
                "content": html_content,
                "description": description,
                "guid": article_url,
            }
            new_items.append(item)

            if mark_read or archive:
                try:
                    mark_message_processed(service, msg_id, mark_read, archive)
                except Exception as e:
                    print(f"\n  Warning: could not mark message {msg_id}: {e}")

        except Exception as e:
            print(f"\n  Warning: could not process message {msg_id}: {e}")

    print(f"\nParsed {len(new_items)} new items")

    all_items = new_items + existing_items
    all_items.sort(key=lambda x: x["pub_date"], reverse=True)
    all_items = all_items[: feed_cfg["max_items"]]

    rss_items = []
    for item in all_items:
        rss_item = dict(item)
        if isinstance(rss_item["pub_date"], str):
            rss_item["pub_date"] = datetime.fromisoformat(rss_item["pub_date"])
        rss_items.append(rss_item)

    rss_xml = generate_rss_xml(feed_cfg, rss_items, repo_base_url)
    output_path = feeds_dir / f"{feed_cfg['hash']}.xml"
    output_path.write_text(rss_xml, encoding="utf-8")
    print(f"Written → {output_path.name}")

    all_seen = list(set(seen_ids) | set(all_new_ids))
    state[name] = {"seen_ids": all_seen, "items": all_items}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config_path = Path(__file__).parent.parent / "config.json"
    with open(config_path) as f:
        config = json.load(f)

    repo_base_url = config.get("repo_base_url", "")
    feeds_dir = Path(__file__).parent.parent / "feeds"
    feeds_dir.mkdir(exist_ok=True)

    print("Authenticating with Gmail API...")
    service = build_gmail_service()
    print("Authenticated.")

    state = load_state()

    mark_read = config.get("mark_read", False)
    archive = config.get("archive", False)

    for feed_cfg in config["feeds"]:
        if feed_cfg.get("type") == "read_later":
            process_read_later_feed(service, feed_cfg, feeds_dir, repo_base_url, state, mark_read, archive)
        else:
            process_feed(service, feed_cfg, feeds_dir, repo_base_url, state, mark_read, archive)

    save_state(state)
    print(f"\n{'='*60}")
    print("All feeds updated.")


if __name__ == "__main__":
    main()
