#!/usr/bin/env python3
"""Fetch Gmail newsletter emails and generate RSS feeds.

Reads config.json to determine which Gmail labels to fetch,
then writes RSS 2.0 XML files to the feeds/ directory.
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
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# Gmail fetching
# ---------------------------------------------------------------------------

def get_label_id(service, label_name):
    """Return the Gmail label ID for a given label name.

    Supports nested labels like 'Newsletters/Message Box'.
    """
    result = service.users().labels().list(userId="me").execute()
    for label in result.get("labels", []):
        if label["name"].lower() == label_name.lower():
            return label["id"]
    raise ValueError(f"Gmail label not found: '{label_name}'")


def fetch_message_ids(service, label_id, max_items):
    """Return a list of message ID dicts for all messages with the label."""
    messages = []
    page_token = None

    while len(messages) < max_items:
        kwargs = {
            "userId": "me",
            "labelIds": [label_id],
            "maxResults": min(500, max_items - len(messages)),
        }
        if page_token:
            kwargs["pageToken"] = page_token

        result = service.users().messages().list(**kwargs).execute()
        messages.extend(result.get("messages", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    return messages[:max_items]


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
    # Add base64 padding
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
    """
    # 1. List-Post header
    url = extract_url_from_list_post(msg.get("List-Post", ""))
    if url:
        return url

    # 2. "View this post on the web at <url>" in the plain text
    match = re.search(
        r"view this post on the web at\s+(https?://\S+)", plain_text, re.IGNORECASE
    )
    if match:
        return match.group(1).rstrip("?")

    # 3. Decode the first Substack redirect URL found anywhere in the body
    match = re.search(r"https?://substack\.com/redirect/\S+", plain_text)
    if match:
        url = decode_substack_redirect(match.group(0))
        if url:
            return url

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
    # If the paragraph shrinks to <120 chars after removing invisible chars,
    # or contains "View in browser", treat it as boilerplate
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

    # Drop leading paragraphs that are Substack pre-header padding.
    # These appear when the email client omits "View this post" but keeps
    # the invisible-char spacer paragraph.
    paragraphs = re.split(r"\n\n+", text)
    while paragraphs and is_preheader_paragraph(paragraphs[0]):
        paragraphs = paragraphs[1:]
    text = "\n\n".join(paragraphs).strip()

    return text


def strip_newsletter_intro(text, patterns):
    """Strip leading paragraphs whose text contains any of the given strings.

    Used for newsletters that open every issue with the same boilerplate
    paragraphs (e.g. 'Welcome back to One First...').  Stripping continues
    as long as the first remaining paragraph matches; the moment a paragraph
    doesn't match, the rest of the content is kept untouched.
    """
    if not patterns:
        return text
    paragraphs = re.split(r"\n\n+", text.strip())
    while paragraphs:
        first_lower = paragraphs[0].lower()
        if any(p.lower() in first_lower for p in patterns):
            paragraphs.pop(0)
        else:
            break
    return "\n\n".join(paragraphs).strip()


def text_to_html(text):
    """Convert cleaned plain text to simple HTML suitable for an RSS reader.

    Rules:
    - Double-newline boundaries become paragraph breaks.
    - A single-line paragraph that is short and ends without sentence-ending
      punctuation is treated as a section heading (<h3>).
    - Inline Substack redirect links [ https://... ] are stripped.
    """
    # Strip inline [ url ] tracking links
    text = re.sub(r"\s*\[\s*https?://\S+?\s*\]", "", text)
    # Strip any remaining invisible chars
    text = strip_invisible_chars(text)

    paragraphs = re.split(r"\n\n+", text.strip())
    html_parts = []

    for para in paragraphs:
        para = strip_invisible_chars(para).strip()
        if not para:
            continue

        # Collapse soft-wrapped lines within a paragraph into a single line
        lines = [ln.strip() for ln in para.split("\n") if ln.strip()]
        if not lines:
            continue

        joined = " ".join(lines)

        # Skip if the paragraph is just a URL (e.g. a stray "View in browser" link)
        if re.match(r"^https?://\S+$", joined):
            continue

        # Heuristic: treat as a section heading if it's a single short line
        # that doesn't end with sentence punctuation
        is_heading = (
            len(lines) == 1
            and len(joined) < 80
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
    # Strip tags
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

    # Sort newest-first, cap at max_items
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
# Main
# ---------------------------------------------------------------------------

def process_feed(service, feed_cfg, feeds_dir, repo_base_url):
    """Fetch emails for one feed config entry and write the RSS XML file."""
    print(f"\n{'='*60}")
    print(f"Feed: {feed_cfg['name']}")
    print(f"Label: {feed_cfg['label']}")

    try:
        label_id = get_label_id(service, feed_cfg["label"])
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return

    message_refs = fetch_message_ids(service, label_id, feed_cfg["max_items"])
    print(f"Found {len(message_refs)} messages")

    items = []
    for i, msg_ref in enumerate(message_refs):
        print(f"  Parsing message {i + 1}/{len(message_refs)}...", end="\r")
        try:
            msg = fetch_raw_message(service, msg_ref["id"])

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
            clean_text = strip_header_footer(plain_text)
            clean_text = strip_newsletter_intro(
                clean_text, feed_cfg.get("strip_intro_containing", [])
            )
            html_content = text_to_html(clean_text)
            description = make_description(html_content)

            # Use canonical URL as GUID; fall back to Message-Id
            guid = url if url else f"gmail:{msg.get('Message-Id', msg_ref['id'])}"

            items.append(
                {
                    "title": subject,
                    "url": url,
                    "pub_date": pub_date,
                    "author": author,
                    "content": html_content,
                    "description": description,
                    "guid": guid,
                }
            )
        except Exception as e:
            print(f"\n  Warning: could not parse message {msg_ref['id']}: {e}")

    print(f"\nParsed {len(items)} items successfully")

    rss_xml = generate_rss_xml(feed_cfg, items, repo_base_url)

    output_path = feeds_dir / f"{feed_cfg['hash']}.xml"
    output_path.write_text(rss_xml, encoding="utf-8")
    print(f"Written → {output_path.name}")


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

    for feed_cfg in config["feeds"]:
        process_feed(service, feed_cfg, feeds_dir, repo_base_url)

    print(f"\n{'='*60}")
    print("All feeds updated.")


if __name__ == "__main__":
    main()
