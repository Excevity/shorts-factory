#!/usr/bin/env python3
"""
Faceless Shorts Factory  -  original content edition
====================================================

A zero-cost, fully automated YouTube Shorts pipeline for GitHub Actions.

Nothing is scraped from YouTube, so there is nothing for Google to IP-block.
Every Short is original: our script, our narration, licensed stock footage.

Pipeline
--------
1. SOURCE   : Read gaming / tech RSS feeds for today's stories. Feeds are
              designed for machines, so datacenter IPs are welcome.
2. SCRIPT   : Google Gemini (free tier) turns one story into a tight 45-55
              second narration plus title, description, hashtags and a list
              of b-roll search terms.
3. VOICE    : Microsoft Edge's online TTS reads the script. Free, no API key.
              It also returns word-level timings, which we turn into captions
              WITHOUT needing any speech recognition.
4. FOOTAGE  : Pexels (free API) supplies vertical stock clips matching the
              b-roll terms. If no key is set, we generate an animated
              gradient background instead so the run still works.
5. ASSEMBLE : FFmpeg builds 1080x1920, burns in the captions, muxes audio.
6. UPLOAD   : YouTube Data API v3 resumable upload.

Run locally for testing with:  python main.py --dry-run

Author: generated for Ege
License: MIT
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("shorts-factory")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PipelineError(RuntimeError):
    """Base class for every failure this pipeline raises on purpose."""


class ConfigError(PipelineError):
    """A required secret / environment variable is missing or malformed."""


class NoStoryError(PipelineError):
    """Nothing fresh to talk about. Not a crash - just a quiet day."""


class ScriptError(PipelineError):
    """Gemini refused or returned something unusable for THIS story."""


class RateLimitError(PipelineError):
    """Gemini's free-tier quota is exhausted. Retrying other stories will not
    help - it would only burn more quota - so this aborts the whole run."""


class NarrationError(PipelineError):
    """Text-to-speech failed."""


class FootageError(PipelineError):
    """Could not obtain any usable background footage."""


class RenderError(PipelineError):
    """FFmpeg failed to produce a valid video."""


class UploadError(PipelineError):
    """YouTube rejected the upload."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_FEEDS = ",".join([
    # --- gaming -----------------------------------------------------------
    "https://feeds.ign.com/ign/games-all",
    "https://www.pcgamer.com/rss/",
    "https://www.eurogamer.net/feed",
    "https://www.rockpapershotgun.com/feed",
    "https://www.gamespot.com/feeds/news/",
    "https://www.polygon.com/rss/index.xml",
    "https://www.nintendolife.com/feeds/latest",
    "https://blog.playstation.com/feed/",
    # --- tech / AI --------------------------------------------------------
    "https://techcrunch.com/category/artificial-intelligence/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://feeds.arstechnica.com/arstechnica/index",
])


def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if value is not None:
        value = value.strip()
    if required and not value:
        raise ConfigError(
            f"Missing required environment variable/secret: {name}. "
            "Add it under Settings -> Secrets and variables -> Actions."
        )
    return value or None


@dataclass
class Config:
    # --- credentials -------------------------------------------------------
    gemini_api_key: str
    yt_client_id: str
    yt_client_secret: str
    yt_refresh_token: str
    pexels_api_key: str | None = None       # optional - falls back to gradient
    pixabay_api_key: str | None = None      # optional second footage library

    # --- tuning ------------------------------------------------------------
    feeds: list[str] = field(default_factory=list)
    hours_back: int = 48
    max_stories: int = 25
    target_seconds: int = 50
    max_seconds: int = 59
    # Brian Multilingual is the most conversational of Edge's free voices.
    # Andrew is accurate but flat; Ava is the most expressive alternative.
    voice: str = "en-US-BrianMultilingualNeural"
    fallback_voice: str = "en-US-AvaMultilingualNeural"
    speech_rate: str = "+15%"
    speech_pitch: str = "+8Hz"
    google_tts_api_key: str | None = None
    google_voice: str = "en-US-Chirp3-HD-Charon"
    # Background is deliberately hypnotic filler; cutaways carry the meaning.
    background_queries: list[str] = field(default_factory=list)
    # Pixabay video IDs you have personally approved. Set once, used first.
    curated_clip_ids: list[str] = field(default_factory=list)
    # Strict list for the satisfying background footage.
    footage_blocklist: list[str] = field(default_factory=list)
    # Light list for topic cutaways - must NOT block laptops, phones, offices
    # and so on, which are exactly what a tech cutaway wants to show.
    cutaway_blocklist: list[str] = field(default_factory=list)
    upload_privacy: str = "private"
    # Tried in order. If one is rate limited or retired, the next is used.
    gemini_models: list[str] = field(default_factory=list)
    # New York hours we aim to post at, and how late a run may arrive and still
    # count. GitHub's scheduler is frequently 30-90 minutes late.
    post_hours: list[int] = field(default_factory=lambda: [10, 12, 14])
    grace_minutes: int = 115
    # Latest New York hour at which a missed earlier slot may still be caught up.
    catch_up_until: int = 20
    state_file: Path = Path("state/processed.json")
    output_dir: Path = Path("output")
    dry_run: bool = False

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "Config":
        feeds_raw = _env("RSS_FEEDS", DEFAULT_FEEDS) or DEFAULT_FEEDS
        feeds = [f.strip() for f in feeds_raw.split(",") if f.strip().startswith("http")]
        if not feeds:
            raise ConfigError("RSS_FEEDS contained no valid http(s) URLs.")

        privacy = (_env("UPLOAD_PRIVACY", "private") or "private").lower()
        if privacy not in {"private", "unlisted", "public"}:
            raise ConfigError("UPLOAD_PRIVACY must be private, unlisted or public.")

        def _int(name: str, default: int) -> int:
            try:
                return int(_env(name, str(default)) or default)
            except ValueError as exc:
                raise ConfigError(f"{name} must be a whole number.") from exc

        return cls(
            gemini_api_key=_env("GEMINI_API_KEY", required=not dry_run) or "",
            yt_client_id=_env("YT_CLIENT_ID", required=not dry_run) or "",
            yt_client_secret=_env("YT_CLIENT_SECRET", required=not dry_run) or "",
            yt_refresh_token=_env("YT_REFRESH_TOKEN", required=not dry_run) or "",
            pexels_api_key=_env("PEXELS_API_KEY"),
            pixabay_api_key=_env("PIXABAY_API_KEY"),
            curated_clip_ids=[
                i.strip() for i in (_env("CURATED_CLIP_IDS", "") or "").split(",")
                if i.strip().isdigit()
            ],
            feeds=feeds,
            hours_back=_int("HOURS_BACK", 48),
            max_stories=_int("MAX_STORIES", 25),
            target_seconds=_int("TARGET_SECONDS", 50),
            max_seconds=_int("MAX_SECONDS", 59),
            voice=(_env("TTS_VOICE", "en-US-BrianMultilingualNeural")
                   or "en-US-BrianMultilingualNeural"),
            fallback_voice=(_env("TTS_FALLBACK_VOICE", "en-US-AvaMultilingualNeural")
                            or "en-US-AvaMultilingualNeural"),
            speech_rate=_env("TTS_RATE", "+15%") or "+15%",
            speech_pitch=_env("TTS_PITCH", "+8Hz") or "+8Hz",
            google_tts_api_key=_env("GOOGLE_TTS_API_KEY"),
            google_voice=(_env("GOOGLE_TTS_VOICE", "en-US-Chirp3-HD-Charon")
                          or "en-US-Chirp3-HD-Charon"),
            background_queries=[
                q.strip() for q in (
                    _env("BACKGROUND_QUERIES",
                         # Terms Pixabay genuinely indexes for this genre.
                         # 'asmr' alone is the single strongest signal - the tag
                         # is curated, so anything carrying it is on-genre.
                         # Avoid pairing a genre word with a generic verb:
                         # 'asmr cutting' matches every vegetable-chopping clip.
                         "asmr,kinetic sand,soap cutting,cutting soap,asmr slime,"
                         "slime,kinetic sand cutting,soap carving,sand cutting,"
                         "foam cutting,slime stretching,paint mixing,"
                         "hydraulic press,marble run,pottery wheel")
                    or ""
                ).split(",") if q.strip()
            ],
            footage_blocklist=[
                w.strip().lower() for w in (
                    _env("FOOTAGE_BLOCKLIST",
                         # People doing things, plus scenery - the two ways junk
                         # has actually got through (waterfalls, feet washing).
                         "eating,drinking,smiling,posing,portrait,model,dancing,selfie,"
                         "couple,family,child,kid,baby,face,makeup,fashion,yoga,workout,"
                         "talking,laughing,walking,sitting,girl,woman,boy,man,teenager,"
                         "feet,foot,washing,cleaning,shower,bath,hair,skin,"
                         "waterfall,nature,landscape,mountain,forest,ocean,sea,river,"
                         "beach,sunset,sunrise,sky,cloud,tree,flower,garden,animal,"
                         "dog,cat,bird,street,city,traffic,car,building,office,party")
                    or ""
                ).split(",") if w.strip()
            ],
            cutaway_blocklist=[
                w.strip().lower() for w in (
                    _env("CUTAWAY_BLOCKLIST",
                         # Deliberately short: cutaways are topical, so laptops,
                         # phones, offices and servers must be allowed through.
                         "selfie,posing,model,portrait,makeup,fashion,dancing,"
                         "kissing,wedding,baby,toddler")
                    or ""
                ).split(",") if w.strip()
            ],
            upload_privacy=privacy,
            gemini_models=[
                m.strip() for m in (
                    _env("GEMINI_MODELS",
                         "gemini-2.5-flash,gemini-2.0-flash,gemini-2.5-flash-lite")
                    or ""
                ).split(",") if m.strip()
            ] or ["gemini-2.5-flash"],
            post_hours=[
                int(h) for h in (_env("POST_HOURS", "10,12,14") or "").split(",")
                if h.strip().isdigit()
            ] or [10, 12, 14],
            grace_minutes=_int("GRACE_MINUTES", 115),
            catch_up_until=_int("CATCH_UP_UNTIL", 20),
            state_file=Path(_env("STATE_FILE", "state/processed.json")
                            or "state/processed.json"),
            output_dir=Path(_env("OUTPUT_DIR", "output") or "output"),
            dry_run=dry_run,
        )


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def retry(times: int = 3, delay: float = 2.0, backoff: float = 2.0,
          exceptions: tuple[type[BaseException], ...] = (Exception,)):
    """Decorator: retry a flaky network call with exponential backoff."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            wait, last = delay, None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last = exc
                    if attempt == times:
                        break
                    pause = wait + random.uniform(0, 1.0)
                    log.warning("%s failed (%d/%d): %s - retrying in %.1fs",
                                fn.__name__, attempt, times, exc, pause)
                    time.sleep(pause)
                    wait *= backoff
            raise last  # type: ignore[misc]

        wrapper.__name__ = fn.__name__
        return wrapper

    return decorator


def strip_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def sanitize_filename(name: str, limit: int = 60) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._-")
    return (cleaned or "short")[:limit]


def story_id(link: str) -> str:
    return hashlib.sha1(link.encode("utf-8", "ignore")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# State - so the same story is never covered twice
# ---------------------------------------------------------------------------


class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.processed: set[str] = set()
        self.slots: set[str] = set()      # e.g. "2026-07-31:10am"
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                self.processed = set(raw.get("processed_story_ids", [])
                                     or raw.get("processed_video_ids", []))
                self.slots = set(raw.get("completed_slots", []))
                log.info("Loaded state: %d story/stories covered, %d slot(s) logged.",
                         len(self.processed), len(self.slots))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read state file (%s) - starting fresh.", exc)

    def seen(self, key: str) -> bool:
        return key in self.processed

    def mark(self, key: str) -> None:
        self.processed.add(key)

    def slot_done(self, slot_key: str) -> bool:
        return slot_key in self.slots

    def mark_slot(self, slot_key: str) -> None:
        self.slots.add(slot_key)

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "processed_story_ids": sorted(self.processed)[-800:],
                "completed_slots": sorted(self.slots)[-40:],
            }, indent=2), encoding="utf-8")
            log.info("State saved to %s", self.path)
        except OSError as exc:
            log.error("Failed to write state file: %s", exc)


# ---------------------------------------------------------------------------
# STEP 1 - Source stories from RSS
# ---------------------------------------------------------------------------


@dataclass
class Story:
    title: str
    summary: str
    link: str
    source: str
    published: datetime | None = None

    @property
    def key(self) -> str:
        return story_id(self.link)

    @property
    def age_hours(self) -> float:
        if not self.published:
            return 999.0
        return (datetime.now(timezone.utc) - self.published).total_seconds() / 3600


class NewsSource:
    """Pulls candidate stories from RSS feeds.

    RSS exists to be read by machines, so unlike scraping YouTube this works
    perfectly well from a datacenter IP.
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def collect(self) -> list[Story]:
        try:
            import feedparser  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ConfigError("feedparser is not installed.") from exc

        stories: dict[str, Story] = {}
        feeds = list(self.cfg.feeds)
        random.shuffle(feeds)

        for url in feeds:
            try:
                resp = requests.get(url, headers=self.HEADERS, timeout=25)
                resp.raise_for_status()
                parsed = feedparser.parse(resp.content)
            except Exception as exc:
                log.warning("Feed failed (%s): %s", url, str(exc)[:120])
                continue

            entries = parsed.entries or []
            log.info("%-52s %d entries", url.split("//")[-1][:52], len(entries))
            source_name = strip_html(getattr(parsed.feed, "title", "")) or url

            for entry in entries[:20]:
                story = self._to_story(entry, source_name)
                if story and story.key not in stories:
                    stories[story.key] = story

        if not stories:
            raise NoStoryError(
                "No RSS feed returned any usable entries. Check the RSS_FEEDS "
                "setting - one or more URLs may have moved."
            )

        fresh = [s for s in stories.values() if s.age_hours <= self.cfg.hours_back]
        if not fresh:  # feeds without dates, or a quiet news period
            log.info("Nothing inside the %dh window - using everything found.",
                     self.cfg.hours_back)
            fresh = list(stories.values())

        fresh.sort(key=lambda s: s.age_hours)
        log.info("Collected %d story/stories (%d fresh).", len(stories), len(fresh))
        return fresh[: self.cfg.max_stories]

    def _to_story(self, entry: Any, source_name: str) -> Story | None:
        title = strip_html(getattr(entry, "title", ""))
        link = (getattr(entry, "link", "") or "").strip()
        if not title or not link or len(title) < 15:
            return None

        summary = strip_html(
            getattr(entry, "summary", "") or getattr(entry, "description", "")
        )
        for content in getattr(entry, "content", []) or []:
            value = strip_html(content.get("value", ""))
            if len(value) > len(summary):
                summary = value

        published = None
        for attr in ("published_parsed", "updated_parsed"):
            parsed_time = getattr(entry, attr, None)
            if parsed_time:
                try:
                    published = datetime(*parsed_time[:6], tzinfo=timezone.utc)
                    break
                except (TypeError, ValueError):
                    pass

        return Story(title=title, summary=summary[:2500], link=link,
                     source=source_name, published=published)


# ---------------------------------------------------------------------------
# STEP 2 - Gemini writes the script
# ---------------------------------------------------------------------------


@dataclass
class Cutaway:
    phrase: str
    query: str
    start: float = 0.0
    end: float = 0.0
    clip: Path | None = None


@dataclass
class VideoScript:
    narration: str
    title: str
    description: str
    hashtags: list[str]
    cutaways: list[Cutaway] = field(default_factory=list)

    @property
    def word_count(self) -> int:
        return len(self.narration.split())


PROMPT = """You write viral short-form video scripts about {topic}.

Below is a news story. Turn it into a narration script for a {target}-second
vertical video (YouTube Shorts / TikTok style).

HARD RULES for `narration`:
- Between {min_words} and {max_words} words. This is critical - it must fit in
  {target} seconds of speech.
- Open with a hook in the first sentence that makes someone stop scrolling.
  A surprising number, a bold claim, or a direct question.
- Plain spoken English. Short sentences. No headings, no bullet points, no
  stage directions, no emoji, no "welcome back to the channel".
- Do NOT write anything that is not meant to be spoken aloud.
- Never invent facts. Only use what the story below actually says. If the story
  is thin, say less rather than making things up.
- End with a punchy closing line or a question to drive comments.

Also produce:
- `title`: under 80 characters, hooky, at most one emoji.
- `description`: 1-2 sentences.
- `hashtags`: 4-6 lowercase tags, no '#' symbol.
- `cutaways`: 3 or 4 objects. Each marks a moment where the video should cut
  from the background to a relevant visual. Each object has:
    * `phrase`: 2-4 words copied EXACTLY as they appear in your narration.
      They must be a literal substring of the narration, or the cut is skipped.
      Pick the most visually concrete moments.
    * `query`: a short, generic stock-footage search for that moment
      (e.g. "gaming setup rgb", "server room", "person counting money").
      No proper nouns or brand names - a stock library returns nothing for
      "Palworld" but plenty for "trading cards".
  Spread them across the script; do not cluster them all at the start.

Return ONLY raw JSON, no markdown fences, exactly this shape:
{{"narration": "", "title": "", "description": "", "hashtags": ["",""],
  "cutaways": [{{"phrase": "", "query": ""}}]}}

STORY TITLE: {title}
SOURCE: {source}
STORY BODY: {summary}
"""


class ScriptWriter:
    BASE = "https://generativelanguage.googleapis.com/v1beta/models"
    AUTH_MODES = ("header", "bearer", "query")
    # ~150 words per minute is a natural narration pace.
    WORDS_PER_SECOND = 2.5
    # Free tier allows only 5-15 requests per minute, so every call counts.
    MAX_CALLS_PER_RUN = 8
    MAX_WAIT = 75.0

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self._auth_mode: str | None = None
        self._models = list(cfg.gemini_models)
        self._calls = 0

    def write(self, story: Story) -> VideoScript:
        target = self.cfg.target_seconds
        prompt = PROMPT.format(
            topic="video games and technology",
            target=target,
            min_words=int(target * self.WORDS_PER_SECOND * 0.80),
            max_words=int(target * self.WORDS_PER_SECOND * 1.05),
            title=story.title,
            source=story.source,
            summary=story.summary or story.title,
        )
        return self._validate(self._parse(self._generate(prompt)))

    # -- transport ----------------------------------------------------------

    def _send(self, url: str, payload: dict[str, Any], mode: str) -> requests.Response:
        headers: dict[str, str] = {}
        params: dict[str, str] = {}
        key = self.cfg.gemini_api_key
        if mode == "header":
            headers["x-goog-api-key"] = key
        elif mode == "bearer":
            headers["Authorization"] = f"Bearer {key}"
        else:
            params["key"] = key
        return self.session.post(url, params=params, headers=headers,
                                 json=payload, timeout=120)

    def list_models(self) -> list[str]:
        """Ask the key which models it can actually use.

        Worth one extra call when things fail: 'model not available' plus
        'rate limited' usually means the key is attached to a project where the
        Generative Language API was never enabled, and this says so plainly.
        """
        try:
            headers, params = {}, {}
            if self._auth_mode == "query" or self._auth_mode is None:
                params["key"] = self.cfg.gemini_api_key
            if self._auth_mode in ("header", None):
                headers["x-goog-api-key"] = self.cfg.gemini_api_key
            resp = self.session.get(self.BASE, headers=headers, params=params, timeout=30)
            if resp.status_code >= 400:
                log.warning("Could not list models (HTTP %s): %s",
                            resp.status_code, resp.text[:250])
                return []
            names = []
            for model in resp.json().get("models") or []:
                if "generateContent" in (model.get("supportedGenerationMethods") or []):
                    names.append(str(model.get("name", "")).replace("models/", ""))
            return names
        except Exception as exc:
            log.warning("Could not list models: %s", str(exc)[:150])
            return []

    @staticmethod
    def _error_text(resp: requests.Response) -> str:
        """Pull Google's human-readable reason out of an error response."""
        try:
            err = (resp.json().get("error") or {})
            message = str(err.get("message") or "")[:300]
            status = str(err.get("status") or "")
            return f"{status}: {message}" if status else message
        except ValueError:
            return resp.text[:250]

    @staticmethod
    def _retry_after(resp: requests.Response) -> float | None:
        """Google tells us how long to wait; obey it instead of guessing."""
        header = resp.headers.get("Retry-After")
        if header:
            try:
                return float(header)
            except ValueError:
                pass
        try:
            for detail in (resp.json().get("error") or {}).get("details") or []:
                delay = str(detail.get("retryDelay") or "")
                match = re.match(r"([\d.]+)s", delay)
                if match:
                    return float(match.group(1))
        except ValueError:
            pass
        return None

    def _call(self, model: str, payload: dict[str, Any]) -> requests.Response:
        url = f"{self.BASE}/{model}:generateContent"
        # Google is midway through changing key formats: old "AIza" keys work as
        # a ?key= parameter, new "AQ." keys must go in a header. Try each once,
        # then remember whichever worked.
        modes = (self._auth_mode,) if self._auth_mode else self.AUTH_MODES
        resp, rejected = None, []
        for mode in modes:
            self._calls += 1
            resp = self._send(url, payload, mode)
            if resp.status_code in (401, 403) and not self._auth_mode:
                rejected.append(f"{mode}={resp.status_code}")
                continue
            if resp.status_code < 400:
                self._auth_mode = mode
            break

        if resp is None:  # pragma: no cover - defensive
            raise ScriptError("Gemini request was never sent.")
        if resp.status_code in (401, 403) and rejected:
            raise ConfigError(
                f"Gemini rejected your API key with every auth style ({', '.join(rejected)}). "
                f"Key starts with '{self.cfg.gemini_api_key[:4]}'. If it starts with 'AQ.', "
                "make a replacement key in the Google Cloud Console instead of AI Studio - "
                f"see the README. Server said: {resp.text[:250]}"
            )
        return resp

    def _payload(self, prompt: str, model: str) -> dict[str, Any]:
        config: dict[str, Any] = {
            "temperature": 0.85,
            # Was 1200, which truncated replies mid-sentence and produced
            # unparseable JSON. Reasoning models also spend this budget on
            # internal thinking before writing a single visible character.
            "maxOutputTokens": 4096,
            "responseMimeType": "application/json",
        }
        # Turn off "thinking" on models that support it - we want the whole
        # budget spent on the answer, not on hidden reasoning.
        if re.search(r"2\.5|[3-9]\.", model):
            config["thinkingConfig"] = {"thinkingBudget": 0}
        return {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": config,
        }

    def _generate(self, prompt: str) -> str:
        payload: dict[str, Any] = {}

        throttled: list[str] = []
        for model in list(self._models):
            for attempt in (1, 2):
                if self._calls >= self.MAX_CALLS_PER_RUN:
                    raise RateLimitError(
                        f"Used the {self.MAX_CALLS_PER_RUN}-call Gemini budget for this "
                        "run without a usable script. Stopping so the daily free quota "
                        "is not burned. The next slot will try again."
                    )
                payload = self._payload(prompt, model)
                try:
                    resp = self._call(model, payload)
                except requests.RequestException as exc:
                    log.warning("Gemini network error on %s: %s", model, str(exc)[:140])
                    time.sleep(5)
                    continue

                if resp.status_code == 429:
                    reason = self._error_text(resp)
                    wait = self._retry_after(resp)
                    if wait is not None and wait <= self.MAX_WAIT and attempt == 1:
                        log.warning("%s rate limited; Google says wait %.0fs. Reason: %s",
                                    model, wait, reason)
                        time.sleep(wait + 1)
                        continue
                    log.warning("%s rate limited (429). Reason: %s", model, reason)
                    throttled.append(model)
                    self._models = [m for m in self._models if m != model]
                    break

                if resp.status_code == 404 or (
                    resp.status_code == 400 and "not found" in resp.text.lower()
                ):
                    log.warning("Model %s unavailable on this key. Reason: %s",
                                model, self._error_text(resp))
                    self._models = [m for m in self._models if m != model]
                    break

                if resp.status_code >= 400:
                    raise ScriptError(f"Gemini HTTP {resp.status_code}: {resp.text[:250]}")

                data = resp.json()
                candidates = data.get("candidates") or []
                if not candidates:
                    raise ScriptError(
                        f"Gemini returned nothing. {data.get('promptFeedback', {})}"
                    )
                finish = str(candidates[0].get("finishReason") or "")
                parts = (candidates[0].get("content") or {}).get("parts") or []
                text = "".join(p.get("text", "") for p in parts).strip()

                if finish == "MAX_TOKENS":
                    log.warning("%s hit the output limit - the reply is cut short.", model)
                if finish in ("SAFETY", "RECITATION", "PROHIBITED_CONTENT", "BLOCKLIST"):
                    raise ScriptError(
                        f"Gemini refused this story ({finish}). Trying a different story."
                    )
                if not text:
                    raise ScriptError(
                        f"Gemini returned an empty body (finishReason={finish or 'unknown'})."
                    )
                if model != self.cfg.gemini_models[0]:
                    log.info("Script written by fallback model %s.", model)
                return text

        # Everything failed. Spend one more call finding out what this key can
        # actually do - that distinguishes "genuinely busy" from "misconfigured".
        available = self.list_models()
        if available:
            log.info("This key CAN use: %s", ", ".join(available[:14]))
            untried = [m for m in available if m not in throttled]
            hint = (
                f"Your key does have access to {len(available)} model(s). Set "
                f"GEMINI_MODELS in automate.yml to one of these: {', '.join(untried[:5])}."
                if untried else
                "Every model your key can use is currently rate limited - wait it out."
            )
        else:
            hint = (
                "This key could not list ANY usable models, which normally means the "
                "Generative Language API is not enabled on the key's Google Cloud "
                "project (Google reports that as a 429 with a limit of 0, not a clear "
                "error). Fix: Cloud Console -> select the shorts-factory project -> "
                "search 'Generative Language API' -> Enable -> then Credentials -> "
                "Create API key, and use that key as GEMINI_API_KEY."
            )
        raise RateLimitError(
            f"No Gemini model worked (tried: {', '.join(throttled) or 'none'}). {hint}"
        )

    # -- parsing ------------------------------------------------------------

    @staticmethod
    def _salvage(text: str) -> dict[str, Any] | None:
        """Rescue a truncated reply.

        If the model runs out of output budget the JSON has no closing brace,
        so json.loads fails outright. The narration is the only field we truly
        need, and it is written first - so pull it out directly and fall back to
        sensible defaults for the rest rather than binning a usable script.
        """
        match = re.search(r'"narration"\s*:\s*"((?:[^"\\]|\\.)*)', text, flags=re.DOTALL)
        if not match:
            return None
        try:
            narration = json.loads(f'"{match.group(1)}"')
        except json.JSONDecodeError:
            narration = match.group(1).replace('\\"', '"').replace("\\n", " ")
        narration = narration.strip()
        # Drop a trailing partial sentence left behind by the cut.
        cut = max(narration.rfind("."), narration.rfind("!"), narration.rfind("?"))
        if cut > 60:
            narration = narration[: cut + 1]
        if len(narration.split()) < 25:
            return None

        def field(name: str) -> str:
            hit = re.search(rf'"{name}"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
            return hit.group(1).strip() if hit else ""

        def array(name: str) -> list[str]:
            hit = re.search(rf'"{name}"\s*:\s*\[(.*?)\]', text, flags=re.DOTALL)
            return re.findall(r'"([^"]+)"', hit.group(1)) if hit else []

        log.warning("Gemini's reply was cut short - salvaged the narration (%d words).",
                    len(narration.split()))
        return {
            "narration": narration,
            "title": field("title"),
            "description": field("description"),
            "hashtags": array("hashtags"),
            "cutaways": [],   # nice-to-have; the video works fine without them
        }

    @classmethod
    def _parse(cls, raw: str) -> VideoScript:
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        obj: dict[str, Any] | None = None

        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if match:
            try:
                obj = json.loads(match.group(0))
            except json.JSONDecodeError:
                obj = None

        if obj is None:
            obj = cls._salvage(cleaned)
        if obj is None:
            raise ScriptError(
                f"Could not read Gemini's reply (len={len(cleaned)}): {cleaned[:220]}"
            )

        def as_list(value: Any, limit: int) -> list[str]:
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",")]
            return [str(v).strip() for v in (value or []) if str(v).strip()][:limit]

        tags = [re.sub(r"[^a-z0-9]", "", t.lower()) for t in as_list(obj.get("hashtags"), 6)]

        cutaways: list[Cutaway] = []
        for item in (obj.get("cutaways") or [])[:5]:
            if isinstance(item, dict):
                phrase = str(item.get("phrase") or "").strip()
                query = str(item.get("query") or "").strip()
                if phrase and query:
                    cutaways.append(Cutaway(phrase=phrase, query=query))

        return VideoScript(
            narration=str(obj.get("narration") or "").strip(),
            title=str(obj.get("title") or "").strip()[:95],
            description=str(obj.get("description") or "").strip()[:400],
            hashtags=[t for t in tags if t] or ["gaming", "tech", "shorts"],
            cutaways=cutaways,
        )

    def _validate(self, script: VideoScript) -> VideoScript:
        # Strip anything that is clearly not meant to be spoken.
        narration = re.sub(r"\[[^\]]{0,60}\]", " ", script.narration)   # [SFX]
        narration = re.sub(r"\*[^*]{0,60}\*", " ", narration)           # *pause*
        narration = re.sub(r"^\s*(NARRATOR|VO|HOST)\s*:\s*", "", narration,
                           flags=re.IGNORECASE | re.MULTILINE)
        narration = re.sub(r"\s+", " ", narration).strip()
        if not narration:
            raise ScriptError("Gemini produced an empty narration.")

        ceiling = int(self.cfg.max_seconds * self.WORDS_PER_SECOND)
        words = narration.split()
        if len(words) > ceiling:
            # Trim to the last complete sentence that fits.
            trimmed = " ".join(words[:ceiling])
            cut = max(trimmed.rfind("."), trimmed.rfind("!"), trimmed.rfind("?"))
            narration = trimmed[: cut + 1] if cut > 40 else trimmed
            log.info("Narration was %d words - trimmed to %d.",
                     len(words), len(narration.split()))

        if len(narration.split()) < 25:
            raise ScriptError(
                f"Narration is only {len(narration.split())} words - too thin to use."
            )

        script.narration = narration
        if not script.title:
            script.title = " ".join(narration.split()[:9])

        # A cutaway is only usable if its phrase actually appears in the final
        # narration - the trim above may have removed the tail it referred to.
        lowered = narration.lower()
        kept = [c for c in script.cutaways if c.phrase.lower() in lowered]
        if len(kept) != len(script.cutaways):
            log.info("Dropped %d cutaway(s) whose phrase is not in the narration.",
                     len(script.cutaways) - len(kept))
        script.cutaways = kept

        log.info("Script: %d words (~%.0fs) | %s",
                 script.word_count, script.word_count / self.WORDS_PER_SECOND, script.title)
        return script


# ---------------------------------------------------------------------------
# STEP 3 - Narration via Microsoft Edge TTS (free, no API key)
# ---------------------------------------------------------------------------


@dataclass
class SpokenWord:
    text: str
    start: float     # seconds
    end: float


def estimate_timings(text: str, duration: float) -> list[SpokenWord]:
    """Work out when each word is spoken, without any speech recognition.

    Needed because the best voices (Google's Chirp3-HD) do not return word
    timings - they do not even accept SSML marks. We spread the words across
    the known audio length, weighting by word length and adding a beat for
    punctuation, which is what actually makes speech uneven.

    Also acts as a safety net for Edge TTS: if it returns audio but no word
    boundaries, captions still work instead of silently disappearing.
    """
    words = [w for w in re.split(r"\s+", text.strip()) if w]
    if not words or duration <= 0:
        return []

    weights: list[float] = []
    for word in words:
        letters = len(re.sub(r"[^\w]", "", word)) or 1
        weight = 1.0 + letters * 0.85
        if word.endswith((",", ";", ":")):
            weight += 2.5
        elif word.endswith((".", "!", "?")):
            weight += 5.0
        weights.append(weight)

    total = sum(weights) or 1.0
    out: list[SpokenWord] = []
    clock = 0.0
    for word, weight in zip(words, weights):
        span = duration * (weight / total)
        # Leave a sliver of gap so consecutive captions do not visually merge.
        out.append(SpokenWord(word, clock, clock + span * 0.94))
        clock += span
    return out


class Narrator:
    """Turns the script into speech.

    Two engines, best first:
      1. Google Cloud TTS Chirp3-HD - near-human, 1M free characters/month.
         Needs GOOGLE_TTS_API_KEY. No word timings, so we estimate them.
      2. Microsoft Edge TTS - free, no key, more robotic. Returns real word
         timings, which we use when available.
    """

    GOOGLE_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def speak(self, text: str, dest: Path,
              probe_duration=None) -> tuple[Path, list[SpokenWord]]:
        errors: list[str] = []

        if self.cfg.google_tts_api_key:
            try:
                self._google(text, dest)
                seconds = probe_duration(dest) if probe_duration else 0.0
                words = estimate_timings(text, seconds) if seconds else []
                log.info("Narration by Google %s (%.1f KB, %.1fs, %d estimated timings).",
                         self.cfg.google_voice, dest.stat().st_size / 1024,
                         seconds, len(words))
                return dest, words
            except Exception as exc:
                errors.append(f"google: {str(exc)[:200]}")
                log.warning("Google TTS failed, falling back to Edge: %s", str(exc)[:200])

        try:
            words = self._edge(text, dest)
        except Exception as exc:
            errors.append(f"edge: {str(exc)[:200]}")
            raise NarrationError(
                "Text-to-speech failed on every engine. " + " | ".join(errors)
            ) from exc

        if not words:
            seconds = probe_duration(dest) if probe_duration else 0.0
            words = estimate_timings(text, seconds) if seconds else []
            log.warning("Edge returned no word boundaries - estimated %d timings instead.",
                        len(words))
        return dest, words

    # -- engine 1: Google Cloud TTS ----------------------------------------

    def _google(self, text: str, dest: Path) -> None:
        voice = self.cfg.google_voice
        payload = {
            "input": {"text": text},
            "voice": {
                "languageCode": "-".join(voice.split("-")[:2]) or "en-US",
                "name": voice,
            },
            # Chirp3-HD rejects speakingRate/pitch, so we only set the format.
            "audioConfig": {"audioEncoding": "MP3"},
        }
        resp = requests.post(
            self.GOOGLE_URL,
            params={"key": self.cfg.google_tts_api_key},
            json=payload, timeout=120,
        )
        if resp.status_code >= 400:
            try:
                message = (resp.json().get("error") or {}).get("message", "")[:250]
            except ValueError:
                message = resp.text[:250]
            raise NarrationError(f"HTTP {resp.status_code}: {message}")

        import base64

        audio = (resp.json() or {}).get("audioContent")
        if not audio:
            raise NarrationError("Google TTS returned no audio.")
        dest.write_bytes(base64.b64decode(audio))
        if dest.stat().st_size < 8_000:
            raise NarrationError("Google TTS returned a suspiciously tiny file.")

    # -- engine 2: Microsoft Edge TTS --------------------------------------

    def _edge(self, text: str, dest: Path) -> list[SpokenWord]:
        try:
            import edge_tts  # type: ignore  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise NarrationError("edge-tts is not installed.") from exc

        last: Exception | None = None
        for voice in (self.cfg.voice, self.cfg.fallback_voice):
            for attempt in (1, 2):
                try:
                    words = asyncio.run(self._edge_synth(text, voice, dest))
                    if dest.exists() and dest.stat().st_size > 8_000:
                        log.info("Narration by Edge %s (%.1f KB, %d word timings).",
                                 voice, dest.stat().st_size / 1024, len(words))
                        return words
                    last = NarrationError(f"{voice} produced an empty audio file.")
                except Exception as exc:
                    last = exc
                    log.warning("Edge TTS attempt %d with %s failed: %s",
                                attempt, voice, str(exc)[:160])
                time.sleep(3 * attempt)
        raise NarrationError(f"Edge TTS failed for every voice. Last error: {last}")

    async def _edge_synth(self, text: str, voice: str, dest: Path) -> list[SpokenWord]:
        import edge_tts  # type: ignore

        # Pitch is the strongest lever Edge gives us for character - raising it
        # a little makes a flat corporate read sound noticeably more animated.
        try:
            communicate = edge_tts.Communicate(
                text, voice, rate=self.cfg.speech_rate, pitch=self.cfg.speech_pitch
            )
        except TypeError:
            communicate = edge_tts.Communicate(text, voice, rate=self.cfg.speech_rate)
        words: list[SpokenWord] = []
        with dest.open("wb") as handle:
            async for chunk in communicate.stream():
                # edge-tts has changed this label between versions, so match
                # loosely - an exact "WordBoundary" check silently produced
                # zero timings and forced every caption to be estimated.
                kind = str(chunk.get("type") or "").lower()
                if kind == "audio" and chunk.get("data"):
                    handle.write(chunk["data"])
                elif "wordboundary" in kind or kind == "word":
                    # Offsets arrive in 100-nanosecond ticks.
                    start = chunk.get("offset", 0) / 10_000_000
                    dur = chunk.get("duration", 0) / 10_000_000
                    word = (chunk.get("text") or "").strip()
                    if word:
                        words.append(SpokenWord(word, start, start + dur))
        return words


# ---------------------------------------------------------------------------
# STEP 4 - Captions (.ass) built from the word timings
# ---------------------------------------------------------------------------


class CaptionBuilder:
    """Groups spoken words into short on-screen phrases and writes an ASS file."""

    MAX_WORDS = 4
    # At 78px bold, ~18 characters is the widest line that fits inside the
    # 1080px frame once the 70px side margins are taken off. Going wider
    # pushes text off both edges.
    MAX_CHARS = 18

    # ASS colours are &HBBGGRR&, not RGB.
    YELLOW = r"{\c&H00E5FF&}"      # bright amber-yellow for the key word
    WHITE = r"{\c&HFFFFFF&}"

    # No background bar: a thick outline plus a real shadow keeps the text
    # readable on any footage and looks far cleaner.
    ASS_HEADER = textwrap.dedent("""\
        [Script Info]
        ScriptType: v4.00+
        PlayResX: 1080
        PlayResY: 1920
        WrapStyle: 0
        ScaledBorderAndShadow: yes

        [V4+ Styles]
        Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
        Style: Pop,{font},82,&H00FFFFFF,&H00FFFFFF,&H00000000,&HC0000000,-1,0,0,0,100,100,0,0,1,10,5,2,70,70,560,1

        [Events]
        Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
        """)

    def __init__(self, font: str = "DejaVu Sans") -> None:
        self.font = font

    @staticmethod
    def _stamp(seconds: float) -> str:
        seconds = max(0.0, seconds)
        hours, rem = divmod(seconds, 3600)
        mins, secs = divmod(rem, 60)
        return f"{int(hours)}:{int(mins):02d}:{secs:05.2f}"

    def group(self, words: list[SpokenWord]) -> list[tuple[float, float, str]]:
        chunks: list[tuple[float, float, str]] = []
        bucket: list[SpokenWord] = []

        def flush() -> None:
            if not bucket:
                return
            text = " ".join(w.text for w in bucket)
            chunks.append((bucket[0].start, bucket[-1].end, text))
            bucket.clear()

        for word in words:
            candidate = " ".join([*(w.text for w in bucket), word.text])
            too_long = len(bucket) >= self.MAX_WORDS or len(candidate) > self.MAX_CHARS
            # A pause of more than a third of a second reads as a new phrase.
            gap = bucket and (word.start - bucket[-1].end) > 0.35
            if bucket and (too_long or gap):
                flush()
            bucket.append(word)
            if word.text.endswith((".", "!", "?")):
                flush()
        flush()

        # Hold every caption on screen until the next one begins. Timings are
        # often estimated rather than measured, and a caption that vanishes
        # early is what makes subtitles feel like they are racing the voice.
        for i in range(len(chunks) - 1):
            start, _end, text = chunks[i]
            chunks[i] = (start, chunks[i + 1][0], text)
        if chunks:
            start, end, text = chunks[-1]
            chunks[-1] = (start, end + 0.45, text)
        return chunks

    # Short words carry no meaning, so never highlight these.
    FILLER = {
        "the", "a", "an", "and", "but", "or", "of", "to", "in", "on", "at", "it",
        "is", "was", "are", "be", "for", "with", "that", "this", "you", "your",
        "just", "so", "if", "as", "by", "from", "has", "had", "have", "not",
    }

    def _colourise(self, phrase: str) -> str:
        """Uppercase the phrase and pop its most meaningful word in yellow."""
        words = phrase.split()
        if not words:
            return phrase.upper()
        best, best_score = -1, 0
        for i, word in enumerate(words):
            bare = re.sub(r"[^A-Za-z0-9]", "", word).lower()
            if not bare or bare in self.FILLER:
                continue
            # Longest real word wins; digits are always worth highlighting.
            score = len(bare) + (6 if any(c.isdigit() for c in bare) else 0)
            if score > best_score:
                best, best_score = i, score
        out = [w.upper() for w in words]
        if best >= 0:
            out[best] = f"{self.YELLOW}{out[best]}{self.WHITE}"
        return " ".join(out)

    def write(self, words: list[SpokenWord], dest: Path) -> Path | None:
        chunks = self.group(words)
        if not chunks:
            log.warning("No word timings available - the video will have no captions.")
            return None

        lines = [self.ASS_HEADER.format(font=self.font)]
        for start, end, text in chunks:
            safe = text.replace("\\", "").replace("{", "(").replace("}", ")")
            lines.append(
                f"Dialogue: 0,{self._stamp(start)},{self._stamp(end)},Pop,,0,0,0,,"
                f"{{\\fad(60,60)}}{self._colourise(safe)}"
            )
        dest.write_text("\n".join(lines) + "\n", encoding="utf-8")
        log.info("Captions: %d phrases written.", len(chunks))
        return dest


# ---------------------------------------------------------------------------
# STEP 5 - Background footage from Pexels (optional)
# ---------------------------------------------------------------------------


def locate_cutaways(cutaways: list[Cutaway], words: list[SpokenWord],
                    total: float, hold: float = 3.0) -> list[Cutaway]:
    """Find when each cutaway phrase is spoken, using the word timings.

    Matching is done on a normalised word sequence so punctuation and casing
    do not break it. Overlapping cutaways are dropped rather than stacked.
    """
    if not words or not cutaways:
        return []

    def norm(text: str) -> list[str]:
        return [re.sub(r"[^a-z0-9]", "", w.lower())
                for w in text.split() if re.sub(r"[^a-z0-9]", "", w.lower())]

    spoken = [re.sub(r"[^a-z0-9]", "", w.text.lower()) for w in words]
    placed: list[Cutaway] = []

    for cut in cutaways:
        needle = norm(cut.phrase)
        if not needle:
            continue
        for i in range(len(spoken) - len(needle) + 1):
            if spoken[i:i + len(needle)] == needle:
                start = max(0.0, words[i].start - 0.15)
                end = min(total, start + hold)
                if end - start < 1.2:
                    break
                # Never let two cutaways overlap.
                if any(not (end <= p.start or start >= p.end) for p in placed):
                    break
                cut.start, cut.end = start, end
                placed.append(cut)
                break
        else:
            log.info("Cutaway phrase not found in the audio: %r", cut.phrase)

    placed.sort(key=lambda c: c.start)
    for cut in placed:
        log.info("Cutaway %5.1f-%4.1fs  %-22r -> %s",
                 cut.start, cut.end, cut.phrase, cut.query)
    return placed


class FootageFetcher:
    """Finds background and cutaway clips.

    Searches two free libraries. Neither ever returns "no results" - they both
    fall back to loosely-related footage - so relevance is checked here rather
    than trusted. A clean gradient beats footage that has nothing to do with
    the search.
    """

    PEXELS = "https://api.pexels.com/videos/search"
    PIXABAY = "https://pixabay.com/api/videos/"

    def __init__(self, cfg: Config, workdir: Path) -> None:
        self.cfg = cfg
        self.workdir = workdir
        self._used: set[str] = set()

    # -- relevance and content filtering ------------------------------------

    @staticmethod
    def _words(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (text or "").lower())

    # A clip only counts as "satisfying" if it shows one of these actions or
    # materials. This is the gate that keeps waterfalls and sunsets out.
    SATISFYING = {
        "cut", "cutting", "cuts", "slice", "slicing", "sliced", "chop", "chopping",
        "carve", "carving", "shave", "shaving", "shavings", "peel", "peeling",
        "crush", "crushing", "press", "squish", "squeeze", "squeezing", "knead",
        "kneading", "stretch", "stretching", "mix", "mixing", "swirl", "pour",
        "pouring", "melt", "melting", "soap", "slime", "foam", "wax", "resin",
        "clay", "kinetic", "sand", "marble", "marbles", "domino", "dominoes",
        "pencil", "lathe", "pottery", "asmr", "satisfying", "glitter", "paint",
        "chocolate", "hydraulic", "woodturning", "sculpting", "moulding",
    }
    STOPWORDS = {"the", "a", "an", "of", "in", "on", "and", "with", "into"}

    def _terms(self, query: str) -> list[str]:
        return [t for t in self._words(query).split()
                if len(t) >= 3 and t not in self.STOPWORDS]

    @staticmethod
    def _term_hit(term: str, hay_words: list[str]) -> bool:
        """Prefix match in both directions so 'cutting' finds 'cut'/'cuts'."""
        stem = term[:5] if len(term) > 5 else term
        for word in hay_words:
            if word.startswith(stem) or (len(word) >= 3 and stem.startswith(word[:4])):
                return True
        return False

    def _relevance(self, query: str, text: str) -> tuple[int, int]:
        """(terms matched, terms total).

        Callers decide how many must match. The background demands ALL of them,
        because accepting one match is what let 'waterfall' through for
        'water beads' and 'woman washing her feet with soap' through for
        'soap cutting' - each matched exactly one of two terms.
        """
        terms = self._terms(query)
        if not terms:
            return 0, 0
        hay = self._words(text).split()
        return sum(1 for t in terms if self._term_hit(t, hay)), len(terms)

    def _is_satisfying(self, text: str) -> bool:
        return bool(set(self._words(text).split()) & self.SATISFYING)

    def _blocked(self, text: str, words: Iterable[str]) -> set[str]:
        return set(self._words(text).split()) & set(words)

    # -- library searches, normalised to one shape --------------------------

    def _pexels(self, query: str) -> list[dict[str, Any]]:
        if not self.cfg.pexels_api_key:
            return []
        try:
            resp = requests.get(
                self.PEXELS, timeout=30,
                headers={"Authorization": self.cfg.pexels_api_key},
                params={"query": query, "per_page": 15,
                        "orientation": "portrait", "size": "medium"},
            )
            if resp.status_code == 401:
                log.error("Pexels rejected PEXELS_API_KEY.")
                return []
            resp.raise_for_status()
            videos = resp.json().get("videos") or []
        except Exception as exc:
            log.warning("Pexels search failed for %r: %s", query, str(exc)[:120])
            return []

        out = []
        for video in videos:
            best = self._best_file(video.get("video_files") or [])
            if not best:
                continue
            # The URL slug is Pexels' own description of the clip.
            slug = str(video.get("url") or "").rstrip("/").split("/")[-1]
            out.append({
                "key": f"pexels:{video.get('id')}",
                "text": re.sub(r"-\d+$", "", slug).replace("-", " "),
                "duration": video.get("duration") or 0,
                "link": best["link"],
                "source": "pexels",
            })
        return out

    def curated(self) -> list[dict[str, Any]]:
        """Clips you picked yourself, looked up by Pixabay ID.

        Set CURATED_CLIP_IDS once and searching is bypassed entirely for the
        background. No maintenance, no search roulette, and you know exactly
        what every video will look like.
        """
        ids = self.cfg.curated_clip_ids
        if not ids or not self.cfg.pixabay_api_key:
            return []
        try:
            resp = requests.get(
                self.PIXABAY, timeout=30,
                params={"key": self.cfg.pixabay_api_key, "id": ",".join(ids)},
            )
            resp.raise_for_status()
            hits = resp.json().get("hits") or []
        except Exception as exc:
            log.warning("Could not load curated clips: %s", str(exc)[:150])
            return []
        out = self._from_pixabay_hits(hits)
        log.info("Curated list: %d of %d clip(s) available.", len(out), len(ids))
        return out

    def _pixabay(self, query: str) -> list[dict[str, Any]]:
        if not self.cfg.pixabay_api_key:
            return []
        try:
            resp = requests.get(
                self.PIXABAY, timeout=30,
                params={"key": self.cfg.pixabay_api_key, "q": query,
                        "video_type": "film", "per_page": 20, "safesearch": "true"},
            )
            resp.raise_for_status()
            hits = resp.json().get("hits") or []
        except Exception as exc:
            log.warning("Pixabay search failed for %r: %s", query, str(exc)[:120])
            return []
        return self._from_pixabay_hits(hits)

    def _from_pixabay_hits(self, hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for hit in hits:
            streams = hit.get("videos") or {}
            # Prefer a stream tall enough to fill 1080x1920 without upscaling.
            pick = None
            for name in ("large", "medium", "small", "tiny"):
                stream = streams.get(name) or {}
                if stream.get("url") and (stream.get("height") or 0) >= 720:
                    pick = stream
                    break
            if not pick:
                continue
            out.append({
                "key": f"pixabay:{hit.get('id')}",
                # Pixabay gives explicit tags - a much better relevance signal.
                "text": str(hit.get("tags") or ""),
                "duration": hit.get("duration") or 0,
                "link": pick["url"],
                "source": "pixabay",
            })
        return out

    def _candidates(self, query: str, min_seconds: int,
                    mode: str = "background") -> list[dict[str, Any]]:
        """Both libraries, junk removed, best matches first.

        `mode` matters a lot:
          background - must be genuinely satisfying-genre footage, so it also
                       has to contain a word from SATISFYING. Strict on people.
          cutaway    - must simply match the topic. Only obviously
                       people-focused clips are blocked, because a cutaway
                       about tech legitimately wants laptops, phones, offices.
        """
        blocklist = (self.cfg.footage_blocklist if mode == "background"
                     else self.cfg.cutaway_blocklist)
        # Pixabay carries the satisfying/ASMR genre and tags it properly;
        # Pexels barely stocks it and mostly answers with food chopping. So for
        # backgrounds only fall through to Pexels if Pixabay gave us nothing.
        pool = self._pixabay(query)
        if mode != "background" or not pool:
            pool = pool + self._pexels(query)
        scored = []

        for item in pool:
            if item["key"] in self._used or item["duration"] < min_seconds:
                continue
            text = item["text"]

            blocked = self._blocked(text, blocklist)
            if blocked:
                log.info("  skip [%s] %-36s (blocked: %s)",
                         item["source"], text[:36], ", ".join(sorted(blocked)))
                continue

            hits, total = self._relevance(query, text)
            # Background must match every term; a cutaway only needs about half,
            # since Gemini's wording rarely matches a stock caption exactly.
            required = total if mode == "background" else max(1, (total + 1) // 2)
            if total == 0 or hits < required:
                log.info("  skip [%s] %-36s (matched %d/%d of %r)",
                         item["source"], text[:36], hits, total, query)
                continue
            score = hits

            if mode == "background" and not self._is_satisfying(text):
                log.info("  skip [%s] %-36s (not satisfying-genre)",
                         item["source"], text[:36])
                continue

            item["score"] = score
            scored.append(item)

        scored.sort(key=lambda i: i["score"], reverse=True)
        return scored

    # -- public API ---------------------------------------------------------

    def fetch(self, queries: list[str], needed: int) -> list[Path]:
        if not (self.cfg.pexels_api_key or self.cfg.pixabay_api_key):
            log.info("No footage API key set - using a generated gradient background.")
            return []

        clips: list[Path] = []

        # Your approved clips come first. If they cover the video, we never
        # search at all - which is the whole point of curating them.
        picked = self.curated()
        random.shuffle(picked)
        for item in picked:
            if len(clips) >= needed:
                break
            path = self.workdir / f"stock_{len(clips):02d}.mp4"
            if self._stream(item["link"], path):
                self._used.add(item["key"])
                clips.append(path)
                log.info("  using [curated] %s", item["text"][:55])
        if len(clips) >= needed:
            log.info("Background covered by your curated clips - no search needed.")
            return clips

        shuffled = list(queries)
        random.shuffle(shuffled)

        for query in shuffled:
            if len(clips) >= needed:
                break
            candidates = self._candidates(query, min_seconds=4)
            log.info("%-26r -> %d usable clip(s)", query, len(candidates))
            for item in candidates:
                if len(clips) >= needed:
                    break
                path = self.workdir / f"stock_{len(clips):02d}.mp4"
                if self._stream(item["link"], path):
                    self._used.add(item["key"])
                    clips.append(path)
                    log.info("  using [%s] %s", item["source"], item["text"][:50])

        if not clips:
            log.warning("Nothing relevant found in any library - using a gradient "
                        "background instead of unrelated footage.")
        else:
            log.info("Downloaded %d background clip(s).", len(clips))
        return clips

    def fetch_one(self, query: str, tag: str) -> Path | None:
        """A single clip for a keyword cutaway."""
        for item in self._candidates(query, min_seconds=3, mode="cutaway"):
            path = self.workdir / f"cut_{tag}.mp4"
            if self._stream(item["link"], path):
                self._used.add(item["key"])
                log.info("  cutaway [%s] %s", item["source"], item["text"][:50])
                return path
        log.info("No relevant cutaway footage for %r.", query)
        return None

    # -- downloading --------------------------------------------------------

    @staticmethod
    def _best_file(files: list[dict[str, Any]]) -> dict[str, Any] | None:
        usable = [f for f in files
                  if f.get("link") and f.get("file_type") == "video/mp4" and f.get("height")]
        if not usable:
            return None
        portrait = [f for f in usable if (f.get("height") or 0) > (f.get("width") or 0)]
        pool = portrait or usable
        sized = [f for f in pool if 1000 <= (f.get("height") or 0) <= 2200]
        return max(sized or pool, key=lambda f: f.get("height") or 0)

    def _stream(self, url: str, path: Path) -> bool:
        try:
            with requests.get(url, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                size = 0
                with path.open("wb") as handle:
                    for block in resp.iter_content(1 << 16):
                        handle.write(block)
                        size += len(block)
                        if size > 60 * 1024 * 1024:
                            break
            if path.stat().st_size < 40_000:
                path.unlink(missing_ok=True)
                return False
            return True
        except Exception as exc:
            log.warning("Clip download failed: %s", str(exc)[:120])
            path.unlink(missing_ok=True)
            return False




# ---------------------------------------------------------------------------
# STEP 6 - Assemble the vertical video with FFmpeg
# ---------------------------------------------------------------------------


class VideoAssembler:
    WIDTH, HEIGHT, FPS = 1080, 1920, 30

    def __init__(self, cfg: Config, workdir: Path) -> None:
        self.cfg = cfg
        self.workdir = workdir
        self.ffmpeg = shutil.which("ffmpeg")
        self.ffprobe = shutil.which("ffprobe")
        if not self.ffmpeg or not self.ffprobe:
            raise RenderError(
                "ffmpeg/ffprobe not found on PATH. GitHub's ubuntu-latest runner "
                "ships with them; add an apt-get install step if you changed runner."
            )

    # -- helpers ------------------------------------------------------------

    def _run(self, cmd: list[str], label: str, timeout: int = 900) -> None:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise RenderError(f"FFmpeg timed out during {label}.") from exc
        if proc.returncode != 0:
            raise RenderError(f"FFmpeg failed during {label}: {proc.stderr[-1000:]}")

    def duration(self, path: Path) -> float:
        try:
            out = subprocess.run(
                [self.ffprobe, "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=nw=1:nk=1", str(path)],
                capture_output=True, text=True, timeout=60, check=True)
            return float(out.stdout.strip() or 0)
        except (subprocess.SubprocessError, ValueError):
            return 0.0

    # -- background ---------------------------------------------------------

    def _normalise(self, clip: Path, index: int, seconds: float) -> Path | None:
        """Crop/scale one stock clip to a 1080x1920 segment, with a slow zoom."""
        out = self.workdir / f"seg_{index:02d}.mp4"
        chain = (
            f"scale={self.WIDTH}:{self.HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={self.WIDTH}:{self.HEIGHT},fps={self.FPS},"
            "eq=brightness=-0.06:saturation=1.1,setsar=1"
        )
        cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
               "-stream_loop", "-1", "-i", str(clip),
               "-t", f"{seconds:.2f}", "-an", "-vf", chain,
               "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
               "-pix_fmt", "yuv420p", str(out)]
        try:
            self._run(cmd, f"normalising clip {index}", timeout=300)
        except RenderError as exc:
            log.warning("Skipping unusable stock clip %d: %s", index, str(exc)[:140])
            return None
        return out if out.exists() and out.stat().st_size > 20_000 else None

    def _gradient(self, seconds: float) -> Path:
        """Fallback background when we have no stock footage."""
        out = self.workdir / "bg_gradient.mp4"
        palettes = [("0x0f2027", "0x2c5364"), ("0x1a1a2e", "0x533483"),
                    ("0x11998e", "0x0f2027"), ("0x232526", "0x414345")]
        c0, c1 = random.choice(palettes)
        source = (
            f"gradients=s={self.WIDTH}x{self.HEIGHT}:c0={c0}:c1={c1}"
            f":x0=0:y0=0:x1={self.WIDTH}:y1={self.HEIGHT}"
            f":d={max(6, int(seconds))}:speed=0.012:r={self.FPS}"
        )
        cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
               "-f", "lavfi", "-i", source, "-t", f"{seconds:.2f}",
               "-vf", f"format=yuv420p,setsar=1",
               "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", str(out)]
        try:
            self._run(cmd, "gradient background", timeout=300)
            return out
        except RenderError:
            log.warning("gradients filter unavailable - using a flat colour.")
            cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                   "-f", "lavfi",
                   "-i", f"color=c=0x14161c:s={self.WIDTH}x{self.HEIGHT}:r={self.FPS}",
                   "-t", f"{seconds:.2f}", "-vf", "format=yuv420p",
                   "-c:v", "libx264", "-preset", "veryfast", str(out)]
            self._run(cmd, "flat colour background", timeout=300)
            return out

    def build_background(self, clips: list[Path], seconds: float) -> Path:
        if not clips:
            return self._gradient(seconds)

        per_clip = max(3.0, seconds / len(clips))
        segments: list[Path] = []
        for index, clip in enumerate(clips):
            segment = self._normalise(clip, index, per_clip)
            if segment:
                segments.append(segment)

        if not segments:
            return self._gradient(seconds)

        # Repeat the sequence until it is long enough to cover the narration.
        # This must run even for a single segment: if some clips failed to
        # normalise, one surviving segment is shorter than the narration, and
        # a short background silently truncates the audio via -shortest.
        ordered: list[Path] = []
        covered = 0.0
        while covered < seconds + 1 and len(ordered) < 40:
            for segment in segments:
                ordered.append(segment)
                covered += per_clip
                if covered >= seconds + 1:
                    break

        listing = self.workdir / "concat.txt"
        listing.write_text(
            "".join(f"file '{p.resolve().as_posix()}'\n" for p in ordered),
            encoding="utf-8")

        out = self.workdir / "bg.mp4"
        self._run([self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                   "-f", "concat", "-safe", "0", "-i", str(listing),
                   "-t", f"{seconds:.2f}", "-c", "copy", str(out)],
                  "joining background segments")

        made = self.duration(out)
        if made < seconds - 0.5:
            log.warning("Background came out %.1fs for %.1fs of narration - "
                        "falling back to a gradient so nothing gets cut off.",
                        made, seconds)
            return self._gradient(seconds)
        return out

    # -- final mux ----------------------------------------------------------

    def prepare_cutaways(self, cutaways: list[Cutaway]) -> list[Cutaway]:
        """Normalise each cutaway clip to a full-screen segment of exact length."""
        ready: list[Cutaway] = []
        for index, cut in enumerate(cutaways):
            if not cut.clip or not cut.clip.exists():
                continue
            out = self.workdir / f"cutseg_{index:02d}.mp4"
            span = max(1.2, cut.end - cut.start)
            chain = (
                f"scale={self.WIDTH}:{self.HEIGHT}:force_original_aspect_ratio=increase,"
                f"crop={self.WIDTH}:{self.HEIGHT},fps={self.FPS},setsar=1"
            )
            cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
                   "-stream_loop", "-1", "-i", str(cut.clip),
                   "-t", f"{span:.2f}", "-an", "-vf", chain,
                   "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
                   "-pix_fmt", "yuv420p", str(out)]
            try:
                self._run(cmd, f"preparing cutaway {index}", timeout=240)
            except RenderError as exc:
                log.warning("Skipping cutaway %d: %s", index, str(exc)[:140])
                continue
            # Check duration, not file size: a visually simple clip can compress
            # to a few kilobytes and still be perfectly valid.
            made = self.duration(out) if out.exists() else 0.0
            if made >= min(1.0, span * 0.6):
                cut.clip = out
                ready.append(cut)
            else:
                log.warning("Cutaway %d came out %.1fs (wanted %.1fs) - skipping.",
                            index, made, span)
        return ready

    def assemble(self, background: Path, narration: Path, captions: Path | None,
                 dest: Path, cutaways: list[Cutaway] | None = None) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        speech = self.duration(narration)
        cutaways = cutaways or []

        inputs = ["-i", str(background), "-i", str(narration)]
        filters = ["[0:v]scale=%d:%d,setsar=1[base]" % (self.WIDTH, self.HEIGHT)]
        last = "base"

        # Full-screen cutaways: shift each clip onto the main timeline with
        # setpts, then show it only during its window.
        for index, cut in enumerate(cutaways):
            stream = 2 + index          # 0 = background, 1 = narration audio
            inputs += ["-i", str(cut.clip)]
            filters.append(
                f"[{stream}:v]setpts=PTS-STARTPTS+{cut.start:.3f}/TB[cut{index}]"
            )
            filters.append(
                f"[{last}][cut{index}]overlay=0:0:"
                f"enable='between(t,{cut.start:.3f},{cut.end:.3f})'[ov{index}]"
            )
            last = f"ov{index}"

        # No dark band any more - the caption style carries a thick outline and
        # shadow instead, which reads just as well and looks far cleaner.
        if captions:
            escaped = str(captions.resolve()).replace("\\", "/").replace(":", r"\:")
            filters.append(f"[{last}]subtitles='{escaped}'[v]")
        else:
            filters.append(f"[{last}]null[v]")

        cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
               *inputs,
               "-filter_complex", ";".join(filters),
               "-map", "[v]", "-map", "1:a",
               "-c:v", "libx264", "-preset", "medium", "-crf", "21",
               "-profile:v", "high", "-pix_fmt", "yuv420p",
               "-r", str(self.FPS), "-g", str(self.FPS * 2),
               "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
               "-shortest", "-movflags", "+faststart", str(dest)]
        self._run(cmd, "final assembly")

        if not dest.exists() or dest.stat().st_size < 30_000:
            raise RenderError("FFmpeg produced no output (or a tiny broken file).")
        length = self.duration(dest)
        if length > 60.5:
            raise RenderError(
                f"Rendered clip is {length:.1f}s - YouTube will not treat it as a Short."
            )
        # The narration must survive intact. A background shorter than the audio
        # would silently cut the voice off mid-sentence via -shortest.
        if speech and length < min(speech, self.cfg.max_seconds) - 0.75:
            raise RenderError(
                f"Video is {length:.1f}s but the narration is {speech:.1f}s - the "
                "voiceover would be cut off. Refusing to upload a truncated video."
            )
        log.info("Rendered %s (%.1f MB, %.1fs).",
                 dest.name, dest.stat().st_size / 1_048_576, length)
        return dest


# ---------------------------------------------------------------------------
# STEP 7 - Upload to YouTube as a Short
# ---------------------------------------------------------------------------


class YouTubeUploader:
    SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def upload(self, video: Path, script: VideoScript, story: Story) -> str:
        try:
            from googleapiclient.discovery import build  # type: ignore
            from googleapiclient.errors import HttpError  # type: ignore
            from googleapiclient.http import MediaFileUpload  # type: ignore
            from google.auth.transport.requests import Request  # type: ignore
            from google.oauth2.credentials import Credentials  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise UploadError("google-api-python-client is not installed.") from exc

        creds = Credentials(
            token=None,
            refresh_token=self.cfg.yt_refresh_token,
            client_id=self.cfg.yt_client_id,
            client_secret=self.cfg.yt_client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=self.SCOPES,
        )
        try:
            creds.refresh(Request())
        except Exception as exc:
            raise UploadError(
                f"Could not refresh the YouTube OAuth token: {exc}. "
                "Re-generate YT_REFRESH_TOKEN (README step 4), and make sure you "
                "clicked 'Publish app' on the OAuth consent screen."
            ) from exc

        title = script.title if "#short" in script.title.lower() else f"{script.title} #Shorts"
        body = {
            "snippet": {
                "title": title[:100],
                "description": self._description(script, story),
                "tags": (script.hashtags + ["shorts", "gaming", "technology"])[:15],
                "categoryId": "20",   # Gaming
            },
            "status": {
                "privacyStatus": self.cfg.upload_privacy,
                "selfDeclaredMadeForKids": False,
            },
        }

        youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)
        media = MediaFileUpload(str(video), chunksize=4 * 1024 * 1024,
                                resumable=True, mimetype="video/mp4")
        request = youtube.videos().insert(part="snippet,status", body=body,
                                          media_body=media)

        log.info("Uploading to YouTube as %s ...", self.cfg.upload_privacy)
        response, errors = None, 0
        while response is None:
            try:
                status, response = request.next_chunk()
                if status:
                    log.info("  ... %d%% uploaded", int(status.progress() * 100))
            except HttpError as exc:
                if exc.resp.status in (500, 502, 503, 504) and errors < 5:
                    errors += 1
                    time.sleep(2 ** errors)
                    continue
                raise UploadError(f"YouTube rejected the upload: {exc}") from exc
            except Exception as exc:
                errors += 1
                if errors >= 5:
                    raise UploadError(f"YouTube upload failed: {exc}") from exc
                time.sleep(2 ** errors)

        video_id = response.get("id", "")
        log.info("Upload complete: https://youtube.com/shorts/%s", video_id)
        return video_id

    @staticmethod
    def _description(script: VideoScript, story: Story) -> str:
        tags = " ".join(f"#{t}" for t in script.hashtags)
        return (
            f"{script.description}\n\n"
            f"{tags} #Shorts\n\n"
            "-----\n"
            f"Story source: {story.source}\n{story.link}\n\n"
            "Narration and script are original. Stock footage via Pexels."
        )[:4900]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def produce(cfg: Config, workdir: Path, state: State) -> tuple[Path, VideoScript, Story]:
    news = NewsSource(cfg)
    writer = ScriptWriter(cfg)
    narrator = Narrator(cfg)
    captions = CaptionBuilder()
    footage = FootageFetcher(cfg, workdir)
    assembler = VideoAssembler(cfg, workdir)

    stories = news.collect()
    fresh = [s for s in stories if not state.seen(s.key)]
    if not fresh:
        raise NoStoryError(
            "Every story in the feeds has already been covered. "
            "Add more feeds to RSS_FEEDS or raise MAX_STORIES."
        )
    log.info("%d story/stories not yet covered.", len(fresh))

    tally: dict[str, int] = {}
    last_error: Exception | None = None

    # Only a few attempts: each one costs a Gemini call, and the free tier is
    # measured in single-digit requests per minute.
    for index, story in enumerate(fresh[:4], start=1):
        log.info("-" * 70)
        log.info("Story %d: %s", index, story.title)
        log.info("         %s | %.0fh old", story.source, story.age_hours)
        try:
            script = writer.write(story)

            audio, words = narrator.speak(script.narration, workdir / "voice.mp3",
                                          probe_duration=assembler.duration)
            seconds = assembler.duration(audio)
            if seconds < 12:
                raise NarrationError(f"Narration is only {seconds:.1f}s - too short.")
            seconds = min(seconds + 0.4, cfg.max_seconds)
            log.info("Narration length: %.1fs", seconds)

            if not words:
                words = estimate_timings(script.narration, seconds)
            ass = captions.write(words, workdir / "captions.ass")

            # Hypnotic filler background, then topic footage on keyword hits.
            clips = footage.fetch(cfg.background_queries,
                                  needed=max(3, int(seconds // 8)))
            background = assembler.build_background(clips, seconds)

            placed = locate_cutaways(script.cutaways, words, seconds)
            for index, cut in enumerate(placed):
                cut.clip = footage.fetch_one(cut.query, f"{index:02d}")
            placed = assembler.prepare_cutaways([c for c in placed if c.clip])
            log.info("%d cutaway(s) ready.", len(placed))

            dest = cfg.output_dir / f"{sanitize_filename(script.title)}_{story.key}.mp4"
            final = assembler.assemble(background, audio, ass, dest, cutaways=placed)

            state.mark(story.key)
            return final, script, story

        except (ScriptError, NarrationError, FootageError, RenderError) as exc:
            label = type(exc).__name__.replace("Error", "").lower()
            tally[label] = tally.get(label, 0) + 1
            log.warning("Story skipped (%s): %s", label, str(exc)[:220])
            last_error = exc
            # Script failures are about this story; keep it out of the pool.
            if isinstance(exc, ScriptError):
                state.mark(story.key)
            for junk in list(workdir.glob("seg_*.mp4")) + list(workdir.glob("stock_*.mp4")):
                junk.unlink(missing_ok=True)
            continue

    breakdown = ", ".join(f"{n}x {k}" for k, n in sorted(tally.items())) or "none"
    raise NoStoryError(f"Could not produce a video ({breakdown}). Last error: {last_error}")


SLOT_NAMES = {10: "10am", 11: "11am", 12: "12pm", 13: "1pm", 14: "2pm",
              15: "3pm", 16: "4pm", 17: "5pm", 18: "6pm", 19: "7pm",
              8: "8am", 9: "9am"}


def new_york_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        log.warning("No timezone database available - falling back to UTC.")
        return datetime.now(timezone.utc)


def choose_slot(now: datetime, hours: list[int], done: set[str],
                catch_up_until: int = 20) -> tuple[str, str] | None:
    """Pick the slot this run should post for.

    GitHub's scheduler drops runs and delivers others 30-90 minutes late, so
    tying a run to a narrow window around its target hour loses posts outright.
    Instead: take the EARLIEST slot whose time has passed today and which has
    not posted yet. A dropped 10am run is therefore picked up by the next
    wake-up rather than lost for the day.

    `done` is the set of slot keys already completed, so slots are never
    repeated, and catch-up stops at `catch_up_until` so a missed morning does
    not produce a video at midnight.
    """
    today = now.strftime("%Y-%m-%d")
    if now.hour > catch_up_until:
        return None

    for hour in sorted(hours):
        if now < now.replace(hour=hour, minute=0, second=0, microsecond=0):
            continue                      # this slot's time has not arrived yet
        name = SLOT_NAMES.get(hour, f"{hour}h")
        key = f"{today}:{name}"
        if key in done:
            continue                      # already posted
        late = (now - now.replace(hour=hour, minute=0, second=0,
                                  microsecond=0)).total_seconds() / 60
        if late > 20:
            log.info("Catching up the %s slot, %.0f min late.", name, late)
        return key, name
    return None


def write_summary(payload: dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("## Shorts Factory run\n\n")
            for key, value in payload.items():
                handle.write(f"- **{key}**: {value}\n")
            handle.write("\n")
    except OSError:
        pass


def discover(cfg: Config, queries: list[str]) -> int:
    """List candidate background clips so you can pick IDs without a browser.

    Pixabay's website blocks some regions with a 503, but the API answers
    fine from a GitHub runner - so let the runner do the browsing and print
    a table you choose from.
    """
    if not cfg.pixabay_api_key:
        log.error("PIXABAY_API_KEY is not set - cannot search for clips.")
        return 2

    fetcher = FootageFetcher(cfg, Path(tempfile.mkdtemp(prefix="discover_")))
    rows: list[str] = []
    seen: set[str] = set()

    for query in queries:
        hits = fetcher._pixabay(query)
        kept = 0
        for item in hits:
            vid = item["key"].split(":")[1]
            if vid in seen:
                continue
            blocked = fetcher._blocked(item["text"], cfg.footage_blocklist)
            genre = fetcher._is_satisfying(item["text"])
            if blocked or not genre:
                continue
            seen.add(vid)
            kept += 1
            rows.append(f"| `{vid}` | {int(item['duration'])}s | {item['text'][:70]} | "
                        f"https://pixabay.com/videos/id-{vid}/ |")
        log.info("%-28r -> %d good clip(s)", query, kept)

    if not rows:
        log.warning("Nothing matched. Try broader queries.")
        return 0

    ids = ",".join(sorted(r.split("`")[1] for r in rows))
    log.info("\nPaste this into CURATED_CLIP_IDS:\n%s", ids)

    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write("## Candidate background clips\n\n")
            handle.write("Preview any of these, delete the ones you dislike, then "
                         "paste the rest into `CURATED_CLIP_IDS` in `automate.yml`.\n\n")
            handle.write("```\n" + ids + "\n```\n\n")
            handle.write("| ID | Length | Tags | Preview |\n|---|---|---|---|\n")
            handle.write("\n".join(rows) + "\n")
    log.info("Found %d clip(s). See the run summary for a table with preview links.",
             len(rows))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Automated original-content Shorts factory.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build the video but do not upload it.")
    parser.add_argument("--keep-temp", action="store_true",
                        help="Do not delete the temporary working directory.")
    parser.add_argument("--discover", metavar="QUERIES",
                        help="Search for background clips and print their IDs "
                             "instead of making a video. Comma-separated.")
    args = parser.parse_args()

    if args.discover:
        try:
            cfg = Config.from_env(dry_run=True)
        except ConfigError as exc:
            log.error("%s", exc)
            return 2
        wanted = [q.strip() for q in args.discover.split(",") if q.strip()]
        return discover(cfg, wanted or cfg.background_queries)

    started = time.time()
    try:
        cfg = Config.from_env(dry_run=args.dry_run)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2

    slot = os.environ.get("POST_SLOT", "manual")
    log.info("=== Shorts Factory | slot: %s ===", slot)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    state = State(cfg.state_file)

    # Decide which posting slot this run belongs to. Done here rather than in
    # the workflow because it needs the state file: GitHub delivers runs late,
    # so we accept a late run for the slot it missed, but only once per slot.
    slot_key = ""
    if slot != "manual":
        now = new_york_now()
        today = now.strftime("%Y-%m-%d")
        posted = sorted(k.split(":", 1)[1] for k in state.slots if k.startswith(today))
        log.info("New York local time: %s | posted so far today: %s",
                 now.strftime("%Y-%m-%d %H:%M"), ", ".join(posted) or "nothing")

        chosen = choose_slot(now, cfg.post_hours, state.slots, cfg.catch_up_until)
        if not chosen:
            targets = ", ".join(f"{h}:00" for h in sorted(cfg.post_hours))
            log.info("Nothing outstanding (targets: %s). Nothing to do.", targets)
            write_summary({"result": "nothing outstanding",
                           "local time": now.strftime("%H:%M"),
                           "posted today": ", ".join(posted) or "nothing",
                           "targets": targets})
            return 0
        slot_key, slot = chosen
        log.info("This run will post the %s slot.", slot)
    workdir = Path(tempfile.mkdtemp(prefix="shorts_"))
    log.info("Working directory: %s", workdir)

    exit_code = 0
    try:
        video, script, story = produce(cfg, workdir, state)

        if args.dry_run:
            log.info("DRY RUN - not uploading. Video is at %s", video)
            write_summary({"slot": slot, "mode": "dry-run", "title": script.title,
                           "story": story.title, "words": script.word_count,
                           "file": video.name})
        else:
            try:
                video_id = YouTubeUploader(cfg).upload(video, script, story)
                if slot_key:
                    state.mark_slot(slot_key)
                write_summary({
                    "slot": slot,
                    "story": f"{story.title} ({story.source})",
                    "title": script.title,
                    "words": script.word_count,
                    "youtube": f"https://youtube.com/shorts/{video_id}",
                })
            except UploadError as exc:
                log.error("Upload failed: %s", exc)
                write_summary({"slot": slot, "title": script.title,
                               "result": "render ok, upload failed", "detail": str(exc)[:400]})
                exit_code = 1

    except RateLimitError as exc:
        log.warning("Gemini quota exhausted: %s", exc)
        write_summary({"slot": slot, "result": "rate limited (Gemini)",
                       "detail": str(exc)[:500],
                       "what to do": "Nothing - the next slot retries. If it happens "
                                     "every slot, your daily free quota is used up."})
        exit_code = 0   # not a build failure, just a quiet slot
    except NoStoryError as exc:
        log.warning("Nothing posted this run: %s", exc)
        write_summary({"slot": slot, "result": "no story", "detail": str(exc)[:500]})
        exit_code = 0   # a quiet slot is not a build failure
    except ConfigError as exc:
        log.error("Configuration problem: %s", exc)
        write_summary({"slot": slot, "result": "config error", "detail": str(exc)[:500]})
        exit_code = 2
    except PipelineError as exc:
        log.error("Pipeline failure: %s", exc)
        write_summary({"slot": slot, "result": "failed", "detail": str(exc)[:500]})
        exit_code = 1
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
        write_summary({"slot": slot, "result": "crashed", "detail": str(exc)[:500]})
        exit_code = 1
    finally:
        state.save()
        if not args.keep_temp:
            shutil.rmtree(workdir, ignore_errors=True)
        log.info("Finished in %.1fs (exit=%d).", time.time() - started, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
