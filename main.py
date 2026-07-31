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
    """Gemini refused, rate-limited, or returned something unusable."""


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

    # --- tuning ------------------------------------------------------------
    feeds: list[str] = field(default_factory=list)
    hours_back: int = 48
    max_stories: int = 25
    target_seconds: int = 50
    max_seconds: int = 59
    voice: str = "en-US-AndrewNeural"
    fallback_voice: str = "en-US-GuyNeural"
    speech_rate: str = "+8%"
    upload_privacy: str = "private"
    gemini_model: str = "gemini-2.0-flash"
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
            feeds=feeds,
            hours_back=_int("HOURS_BACK", 48),
            max_stories=_int("MAX_STORIES", 25),
            target_seconds=_int("TARGET_SECONDS", 50),
            max_seconds=_int("MAX_SECONDS", 59),
            voice=_env("TTS_VOICE", "en-US-AndrewNeural") or "en-US-AndrewNeural",
            fallback_voice=_env("TTS_FALLBACK_VOICE", "en-US-GuyNeural") or "en-US-GuyNeural",
            speech_rate=_env("TTS_RATE", "+8%") or "+8%",
            upload_privacy=privacy,
            gemini_model=_env("GEMINI_MODEL", "gemini-2.0-flash") or "gemini-2.0-flash",
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
        self._load()

    def _load(self) -> None:
        try:
            if self.path.exists():
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                self.processed = set(raw.get("processed_story_ids", [])
                                     or raw.get("processed_video_ids", []))
                log.info("Loaded state: %d story/stories already covered.",
                         len(self.processed))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read state file (%s) - starting fresh.", exc)

    def seen(self, key: str) -> bool:
        return key in self.processed

    def mark(self, key: str) -> None:
        self.processed.add(key)

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps({
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "processed_story_ids": sorted(self.processed)[-800:],
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
class VideoScript:
    narration: str
    title: str
    description: str
    hashtags: list[str]
    broll: list[str]

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
- `broll`: 5 short visual search phrases for stock footage that match the topic
  (e.g. "gaming setup rgb", "person playing console", "server room"). Generic
  and visual - these are searched against a stock video library, so avoid
  proper nouns and brand names, which return nothing.

Return ONLY raw JSON, no markdown fences, exactly this shape:
{{"narration": "", "title": "", "description": "", "hashtags": ["",""],
  "broll": ["",""]}}

STORY TITLE: {title}
SOURCE: {source}
STORY BODY: {summary}
"""


class ScriptWriter:
    BASE = "https://generativelanguage.googleapis.com/v1beta/models"
    AUTH_MODES = ("header", "bearer", "query")
    # ~150 words per minute is a natural narration pace.
    WORDS_PER_SECOND = 2.5

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self._auth_mode: str | None = None

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

    @retry(times=3, delay=8.0, exceptions=(requests.RequestException, ScriptError))
    def _generate(self, prompt: str) -> str:
        url = f"{self.BASE}/{self.cfg.gemini_model}:generateContent"
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.85,
                "maxOutputTokens": 1200,
                "responseMimeType": "application/json",
            },
        }

        # Google is midway through changing key formats: old "AIza" keys work as
        # a ?key= parameter, new "AQ." keys must go in a header. Try each once,
        # then remember whichever worked.
        modes = (self._auth_mode,) if self._auth_mode else self.AUTH_MODES
        resp, rejected = None, []
        for mode in modes:
            resp = self._send(url, payload, mode)
            if resp.status_code in (401, 403) and not self._auth_mode:
                rejected.append(f"{mode}={resp.status_code}")
                continue
            if resp.status_code < 400:
                self._auth_mode = mode
            break

        if resp is None:  # pragma: no cover - defensive
            raise ScriptError("Gemini request was never sent.")
        if resp.status_code in (401, 403):
            raise ConfigError(
                f"Gemini rejected your API key with every auth style ({', '.join(rejected)}). "
                f"Key starts with '{self.cfg.gemini_api_key[:4]}'. If it starts with 'AQ.', "
                "make a replacement key in the Google Cloud Console instead of AI Studio - "
                f"see the README. Server said: {resp.text[:250]}"
            )
        if resp.status_code == 429:
            raise ScriptError("Gemini free-tier rate limit hit (429).")
        if resp.status_code >= 400:
            raise ScriptError(f"Gemini HTTP {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        candidates = data.get("candidates") or []
        if not candidates:
            raise ScriptError(f"Gemini returned nothing. {data.get('promptFeedback', {})}")
        parts = (candidates[0].get("content") or {}).get("parts") or []
        text = "".join(p.get("text", "") for p in parts).strip()
        if not text:
            raise ScriptError("Gemini returned an empty body.")
        return text

    # -- parsing ------------------------------------------------------------

    @staticmethod
    def _parse(raw: str) -> VideoScript:
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            raise ScriptError(f"No JSON found in Gemini output: {raw[:200]}")
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError as exc:
            raise ScriptError(f"Gemini JSON was malformed: {exc}") from exc

        def as_list(value: Any, limit: int) -> list[str]:
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",")]
            return [str(v).strip() for v in (value or []) if str(v).strip()][:limit]

        tags = [re.sub(r"[^a-z0-9]", "", t.lower()) for t in as_list(obj.get("hashtags"), 6)]

        return VideoScript(
            narration=str(obj.get("narration") or "").strip(),
            title=str(obj.get("title") or "").strip()[:95],
            description=str(obj.get("description") or "").strip()[:400],
            hashtags=[t for t in tags if t] or ["gaming", "tech", "shorts"],
            broll=as_list(obj.get("broll"), 6),
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
        if not script.broll:
            script.broll = ["video game controller", "gaming setup", "computer screen code"]

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


class Narrator:
    """Edge TTS gives us audio AND word-level timings in one pass.

    Those timings are what make burned-in captions possible without running any
    speech recognition - we already know exactly when each word is spoken.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def speak(self, text: str, dest: Path) -> tuple[Path, list[SpokenWord]]:
        try:
            import edge_tts  # type: ignore  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise NarrationError("edge-tts is not installed.") from exc

        last: Exception | None = None
        for voice in (self.cfg.voice, self.cfg.fallback_voice):
            for attempt in (1, 2):
                try:
                    words = asyncio.run(self._synth(text, voice, dest))
                    if dest.exists() and dest.stat().st_size > 8_000:
                        log.info("Narration OK with %s (%.1f KB, %d word timings).",
                                 voice, dest.stat().st_size / 1024, len(words))
                        return dest, words
                    last = NarrationError(f"{voice} produced an empty audio file.")
                except Exception as exc:
                    last = exc
                    log.warning("TTS attempt %d with %s failed: %s",
                                attempt, voice, str(exc)[:160])
                time.sleep(3 * attempt)

        raise NarrationError(
            f"Text-to-speech failed for every voice. Last error: {last}. "
            "Edge's free TTS service occasionally rate-limits; the next run "
            "usually succeeds."
        )

    async def _synth(self, text: str, voice: str, dest: Path) -> list[SpokenWord]:
        import edge_tts  # type: ignore

        communicate = edge_tts.Communicate(text, voice, rate=self.cfg.speech_rate)
        words: list[SpokenWord] = []
        with dest.open("wb") as handle:
            async for chunk in communicate.stream():
                kind = chunk.get("type")
                if kind == "audio" and chunk.get("data"):
                    handle.write(chunk["data"])
                elif kind == "WordBoundary":
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

    ASS_HEADER = textwrap.dedent("""\
        [Script Info]
        ScriptType: v4.00+
        PlayResX: 1080
        PlayResY: 1920
        WrapStyle: 0
        ScaledBorderAndShadow: yes

        [V4+ Styles]
        Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
        Style: Pop,{font},78,&H00FFFFFF,&H00FFFFFF,&H00101010,&H90000000,-1,0,0,0,100,100,0,0,1,7,3,2,70,70,560,1

        [Events]
        Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Encoding, Text
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

        # Close small gaps so captions do not flicker off between phrases.
        for i in range(len(chunks) - 1):
            start, end, text = chunks[i]
            next_start = chunks[i + 1][0]
            if 0 < next_start - end < 0.30:
                chunks[i] = (start, next_start, text)
        return chunks

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
                f"{{\\fad(90,90)}}{safe.upper()}"
            )
        dest.write_text("\n".join(lines) + "\n", encoding="utf-8")
        log.info("Captions: %d phrases written.", len(chunks))
        return dest


# ---------------------------------------------------------------------------
# STEP 5 - Background footage from Pexels (optional)
# ---------------------------------------------------------------------------


class FootageFetcher:
    SEARCH = "https://api.pexels.com/videos/search"

    def __init__(self, cfg: Config, workdir: Path) -> None:
        self.cfg = cfg
        self.workdir = workdir

    def fetch(self, queries: list[str], needed: int) -> list[Path]:
        if not self.cfg.pexels_api_key:
            log.info("No PEXELS_API_KEY set - using a generated gradient background.")
            return []

        session = requests.Session()
        session.headers.update({"Authorization": self.cfg.pexels_api_key})
        clips: list[Path] = []
        seen_ids: set[int] = set()

        for query in queries:
            if len(clips) >= needed:
                break
            try:
                resp = session.get(self.SEARCH, timeout=30, params={
                    "query": query, "per_page": 12,
                    "orientation": "portrait", "size": "medium",
                })
                if resp.status_code == 401:
                    log.error("Pexels rejected PEXELS_API_KEY - falling back to gradient.")
                    return []
                resp.raise_for_status()
                videos = resp.json().get("videos") or []
            except Exception as exc:
                log.warning("Pexels search failed for %r: %s", query, str(exc)[:120])
                continue

            log.info("Pexels %-28r -> %d result(s)", query, len(videos))
            for video in videos:
                if len(clips) >= needed:
                    break
                vid = video.get("id")
                if vid in seen_ids or (video.get("duration") or 0) < 4:
                    continue
                best = self._best_file(video.get("video_files") or [])
                if not best:
                    continue
                path = self._download(best["link"], len(clips), session)
                if path:
                    seen_ids.add(vid)
                    clips.append(path)

        if not clips:
            log.warning("Pexels returned nothing usable - using a gradient background.")
        else:
            log.info("Downloaded %d stock clip(s).", len(clips))
        return clips

    @staticmethod
    def _best_file(files: list[dict[str, Any]]) -> dict[str, Any] | None:
        usable = [f for f in files
                  if f.get("link") and f.get("file_type") == "video/mp4" and f.get("height")]
        if not usable:
            return None
        portrait = [f for f in usable if (f.get("height") or 0) > (f.get("width") or 0)]
        pool = portrait or usable
        # Big enough to fill 1080x1920, but not a needlessly huge download.
        sized = [f for f in pool if 1000 <= (f.get("height") or 0) <= 2200]
        return max(sized or pool, key=lambda f: f.get("height") or 0)

    def _download(self, url: str, index: int, session: requests.Session) -> Path | None:
        path = self.workdir / f"stock_{index:02d}.mp4"
        try:
            with session.get(url, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                size = 0
                with path.open("wb") as handle:
                    for block in resp.iter_content(1 << 16):
                        handle.write(block)
                        size += len(block)
                        if size > 60 * 1024 * 1024:   # don't let one clip run away
                            break
            if path.stat().st_size < 40_000:
                path.unlink(missing_ok=True)
                return None
            return path
        except Exception as exc:
            log.warning("Stock clip download failed: %s", str(exc)[:120])
            path.unlink(missing_ok=True)
            return None


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

    def assemble(self, background: Path, narration: Path,
                 captions: Path | None, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        speech = self.duration(narration)

        # A dark vignette under the captions keeps them readable on any footage.
        filters = [
            "[0:v]scale=%d:%d,setsar=1[base]" % (self.WIDTH, self.HEIGHT),
            "color=c=black@0.38:s=%dx270:r=%d[shade]" % (self.WIDTH, self.FPS),
            "[base][shade]overlay=0:H-800:shortest=1[shaded]",
        ]
        last = "shaded"
        if captions:
            escaped = str(captions.resolve()).replace("\\", "/").replace(":", r"\:")
            filters.append(f"[{last}]subtitles='{escaped}'[v]")
            last = "v"
        else:
            filters.append(f"[{last}]null[v]")
            last = "v"

        cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
               "-i", str(background), "-i", str(narration),
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

    for index, story in enumerate(fresh[:6], start=1):
        log.info("-" * 70)
        log.info("Story %d: %s", index, story.title)
        log.info("         %s | %.0fh old", story.source, story.age_hours)
        try:
            script = writer.write(story)

            audio, words = narrator.speak(script.narration, workdir / "voice.mp3")
            seconds = assembler.duration(audio)
            if seconds < 12:
                raise NarrationError(f"Narration is only {seconds:.1f}s - too short.")
            seconds = min(seconds + 0.4, cfg.max_seconds)
            log.info("Narration length: %.1fs", seconds)

            ass = captions.write(words, workdir / "captions.ass")
            clips = footage.fetch(script.broll, needed=max(3, int(seconds // 6)))
            background = assembler.build_background(clips, seconds)

            dest = cfg.output_dir / f"{sanitize_filename(script.title)}_{story.key}.mp4"
            final = assembler.assemble(background, audio, ass, dest)

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


def main() -> int:
    parser = argparse.ArgumentParser(description="Automated original-content Shorts factory.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build the video but do not upload it.")
    parser.add_argument("--keep-temp", action="store_true",
                        help="Do not delete the temporary working directory.")
    args = parser.parse_args()

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
