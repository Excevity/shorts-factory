#!/usr/bin/env python3
"""
Faceless Shorts Factory
=======================

A zero-cost, fully automated pipeline designed to run inside GitHub Actions.

Pipeline
--------
1. SOURCE   : YouTube Data API v3 search for recent Creative Commons (CC-BY)
              long-form videos in the Tech / AI niche.
2. LOGIC    : Pull the transcript, hand it to Google Gemini (free tier), ask it
              to pick the single most viral continuous 45-60 second segment.
3. PROCESS  : yt-dlp downloads ONLY that timestamped segment, then FFmpeg
              reframes it to a 1080x1920 (9:16) vertical video.
4. DISTRIBUTE: Upload to YouTube as a Short (Data API v3, resumable upload).

Everything is driven by environment variables so that secrets never touch disk.
Run locally for testing with:  python main.py --dry-run

Author: generated for Ege
License: MIT
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
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
    """Base class for every failure this pipeline can raise on purpose."""


class ConfigError(PipelineError):
    """A required secret / environment variable is missing or malformed."""


class NoCandidateError(PipelineError):
    """Nothing usable was found this run. Not a crash - just an empty day."""


class TranscriptError(PipelineError):
    """The transcript could not be fetched for a given video."""


class GeminiError(PipelineError):
    """Gemini refused, rate-limited, or returned something unparseable."""


class DownloadError(PipelineError):
    """yt-dlp could not fetch the requested segment."""


class RenderError(PipelineError):
    """FFmpeg failed to produce a valid vertical clip."""


class UploadError(PipelineError):
    """A distribution target rejected the upload."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if value is not None:
        value = value.strip()
    if required and not value:
        raise ConfigError(
            f"Missing required environment variable/secret: {name}. "
            f"Add it under Settings -> Secrets and variables -> Actions."
        )
    return value or None


@dataclass
class Config:
    # --- credentials -------------------------------------------------------
    youtube_api_key: str
    gemini_api_key: str
    yt_client_id: str
    yt_client_secret: str
    yt_refresh_token: str

    # --- anti-bot helpers (optional) --------------------------------------
    yt_cookies: str | None = None          # raw Netscape cookies.txt contents
    proxy_url: str | None = None           # e.g. http://user:pass@host:port

    # --- tuning ------------------------------------------------------------
    search_queries: list[str] = field(default_factory=list)
    days_back: int = 30
    max_candidates: int = 12
    min_clip_seconds: int = 45
    max_clip_seconds: int = 59
    crop_mode: str = "blur"                # "blur" | "center"
    upload_privacy: str = "private"        # private | unlisted | public
    gemini_model: str = "gemini-2.0-flash"
    state_file: Path = Path("state/processed.json")
    output_dir: Path = Path("output")
    dry_run: bool = False

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "Config":
        queries_raw = _env(
            "SEARCH_QUERIES",
            "artificial intelligence explained,AI tools 2026,machine learning breakthrough,"
            "tech news AI,large language models,AI agents,future of technology",
        )
        queries = [q.strip() for q in (queries_raw or "").split(",") if q.strip()]

        crop_mode = (_env("CROP_MODE", "blur") or "blur").lower()
        if crop_mode not in {"blur", "center"}:
            raise ConfigError("CROP_MODE must be either 'blur' or 'center'.")

        privacy = (_env("UPLOAD_PRIVACY", "private") or "private").lower()
        if privacy not in {"private", "unlisted", "public"}:
            raise ConfigError("UPLOAD_PRIVACY must be private, unlisted or public.")

        def _int(name: str, default: int) -> int:
            try:
                return int(_env(name, str(default)) or default)
            except ValueError as exc:
                raise ConfigError(f"{name} must be an integer.") from exc

        cfg = cls(
            youtube_api_key=_env("YOUTUBE_API_KEY", required=not dry_run) or "",
            gemini_api_key=_env("GEMINI_API_KEY", required=not dry_run) or "",
            yt_client_id=_env("YT_CLIENT_ID", required=not dry_run) or "",
            yt_client_secret=_env("YT_CLIENT_SECRET", required=not dry_run) or "",
            yt_refresh_token=_env("YT_REFRESH_TOKEN", required=not dry_run) or "",
            yt_cookies=_env("YT_COOKIES"),
            proxy_url=_env("PROXY_URL"),
            search_queries=queries,
            days_back=_int("DAYS_BACK", 30),
            max_candidates=_int("MAX_CANDIDATES", 12),
            min_clip_seconds=_int("MIN_CLIP_SECONDS", 45),
            max_clip_seconds=_int("MAX_CLIP_SECONDS", 59),
            crop_mode=crop_mode,
            upload_privacy=privacy,
            gemini_model=_env("GEMINI_MODEL", "gemini-2.0-flash") or "gemini-2.0-flash",
            state_file=Path(_env("STATE_FILE", "state/processed.json") or "state/processed.json"),
            output_dir=Path(_env("OUTPUT_DIR", "output") or "output"),
            dry_run=dry_run,
        )
        return cfg

    @property
    def proxies(self) -> dict[str, str] | None:
        if not self.proxy_url:
            return None
        return {"http": self.proxy_url, "https": self.proxy_url}


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------


def retry(
    times: int = 3,
    delay: float = 2.0,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
):
    """Decorator: retry a flaky network call with exponential backoff."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            wait = delay
            last: BaseException | None = None
            for attempt in range(1, times + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last = exc
                    if attempt == times:
                        break
                    jitter = random.uniform(0, 1.0)
                    log.warning(
                        "%s failed (attempt %d/%d): %s - retrying in %.1fs",
                        fn.__name__, attempt, times, exc, wait + jitter,
                    )
                    time.sleep(wait + jitter)
                    wait *= backoff
            raise last  # type: ignore[misc]

        wrapper.__name__ = fn.__name__
        return wrapper

    return decorator


def hhmmss(seconds: float) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def parse_iso8601_duration(value: str) -> int:
    """Convert an ISO-8601 duration like 'PT1H2M10S' into seconds."""
    match = re.fullmatch(
        r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", value or ""
    )
    if not match:
        return 0
    days, hours, minutes, secs = (int(g) if g else 0 for g in match.groups())
    return days * 86400 + hours * 3600 + minutes * 60 + secs


def sanitize_filename(name: str, limit: int = 60) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._-")
    return (cleaned or "clip")[:limit]


# ---------------------------------------------------------------------------
# State (so we never repost the same source video twice)
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
                self.processed = set(raw.get("processed_video_ids", []))
                log.info("Loaded state: %d video(s) already processed.", len(self.processed))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Could not read state file (%s) - starting fresh.", exc)
            self.processed = set()

    def seen(self, video_id: str) -> bool:
        return video_id in self.processed

    def mark(self, video_id: str) -> None:
        self.processed.add(video_id)

    def save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                # keep the file from growing forever
                "processed_video_ids": sorted(self.processed)[-500:],
            }
            self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            log.info("State saved to %s", self.path)
        except OSError as exc:
            log.error("Failed to write state file: %s", exc)


# ---------------------------------------------------------------------------
# STEP 1 - Sourcing via the YouTube Data API v3
# ---------------------------------------------------------------------------


@dataclass
class Candidate:
    video_id: str
    title: str
    channel: str
    channel_id: str
    published_at: str
    duration_s: int = 0
    view_count: int = 0

    @property
    def url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"

    @property
    def attribution(self) -> str:
        return (
            f'Source: "{self.title}" by {self.channel}\n'
            f"{self.url}\n"
            "Licensed under Creative Commons Attribution (CC BY 3.0)\n"
            "https://creativecommons.org/licenses/by/3.0/"
        )


class YouTubeSource:
    SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
    VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()

    @retry(times=3, delay=3.0, exceptions=(requests.RequestException,))
    def _get(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        params = {**params, "key": self.cfg.youtube_api_key}
        resp = self.session.get(url, params=params, timeout=30)
        if resp.status_code == 403:
            # Quota exhausted or key restricted - retrying will not help.
            raise PipelineError(
                f"YouTube API returned 403. Usually this means the daily quota is "
                f"exhausted or the API key is restricted. Body: {resp.text[:400]}"
            )
        resp.raise_for_status()
        return resp.json()

    def find_candidates(self) -> list[Candidate]:
        """Search several queries, then enrich with duration + view count."""
        published_after = (
            datetime.now(timezone.utc) - timedelta(days=self.cfg.days_back)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        found: dict[str, Candidate] = {}
        queries = list(self.cfg.search_queries)
        random.shuffle(queries)

        for query in queries:
            if len(found) >= self.cfg.max_candidates:
                break
            log.info("Searching YouTube for CC-BY videos: %r", query)
            try:
                data = self._get(
                    self.SEARCH_URL,
                    {
                        "part": "snippet",
                        "q": query,
                        "type": "video",
                        "videoLicense": "creativeCommon",   # <- the CC-BY filter
                        "videoDuration": "medium",          # 4-20 minutes
                        "videoEmbeddable": "true",
                        "relevanceLanguage": "en",
                        "order": "viewCount",
                        "publishedAfter": published_after,
                        "maxResults": 10,
                    },
                )
            except PipelineError:
                raise
            except Exception as exc:  # network gave up after retries
                log.warning("Search failed for %r: %s", query, exc)
                continue

            for item in data.get("items", []):
                vid = (item.get("id") or {}).get("videoId")
                snip = item.get("snippet") or {}
                if not vid or vid in found:
                    continue
                found[vid] = Candidate(
                    video_id=vid,
                    title=snip.get("title", "Untitled"),
                    channel=snip.get("channelTitle", "Unknown channel"),
                    channel_id=snip.get("channelId", ""),
                    published_at=snip.get("publishedAt", ""),
                )

        if not found:
            raise NoCandidateError(
                "YouTube search returned no Creative Commons videos for any query. "
                "Try widening SEARCH_QUERIES or increasing DAYS_BACK."
            )

        self._enrich(found)
        ranked = sorted(found.values(), key=lambda c: c.view_count, reverse=True)
        log.info("Found %d candidate source video(s).", len(ranked))
        return ranked[: self.cfg.max_candidates]

    def _enrich(self, found: dict[str, Candidate]) -> None:
        """Second API call: real duration + view count + license confirmation."""
        ids = list(found.keys())
        for i in range(0, len(ids), 50):
            chunk = ids[i : i + 50]
            try:
                data = self._get(
                    self.VIDEOS_URL,
                    {"part": "contentDetails,statistics,status", "id": ",".join(chunk)},
                )
            except Exception as exc:
                log.warning("Could not enrich video metadata: %s", exc)
                continue

            for item in data.get("items", []):
                cand = found.get(item.get("id", ""))
                if not cand:
                    continue
                details = item.get("contentDetails") or {}
                stats = item.get("statistics") or {}
                status = item.get("status") or {}
                cand.duration_s = parse_iso8601_duration(details.get("duration", ""))
                try:
                    cand.view_count = int(stats.get("viewCount", 0))
                except (TypeError, ValueError):
                    cand.view_count = 0
                # Belt and braces: drop anything the API does not confirm as CC.
                if status.get("license") and status["license"] != "creativeCommon":
                    log.info("Dropping %s - license is %s", cand.video_id, status["license"])
                    found.pop(cand.video_id, None)


# ---------------------------------------------------------------------------
# STEP 2a - Transcript extraction
# ---------------------------------------------------------------------------


@dataclass
class TranscriptCue:
    start: float
    duration: float
    text: str

    @property
    def end(self) -> float:
        return self.start + self.duration


class TranscriptFetcher:
    """
    Gets the caption track for a video.

    Two independent routes, tried in order:
      1. youtube-transcript-api  - fast, but YouTube often blocks it outright
                                   from datacenter IPs like GitHub's runners.
      2. yt-dlp                  - slower, but far more resistant to blocking
                                   and it can use the same cookies/proxy that
                                   the downloader uses.

    Route 2 is why a run can still succeed when route 1 is being IP-blocked.
    """

    LANGS = ("en", "en-US", "en-GB", "en-orig", "a.en")

    def __init__(self, cfg: Config, cookie_file: Path | None = None) -> None:
        self.cfg = cfg
        self.cookie_file = cookie_file
        self.last_reason: str = ""

    def fetch(self, video_id: str) -> list[TranscriptCue]:
        reasons: list[str] = []

        raw = self._fetch_via_api(video_id, reasons)
        if not raw:
            log.info("Falling back to yt-dlp for the transcript of %s ...", video_id)
            raw = self._fetch_via_ytdlp(video_id, reasons)

        if not raw:
            self.last_reason = " | ".join(reasons) or "unknown"
            raise TranscriptError(f"No transcript for {video_id} ({self.last_reason})")

        cues: list[TranscriptCue] = []
        for entry in raw:
            if isinstance(entry, dict):
                text, start, dur = entry.get("text"), entry.get("start"), entry.get("duration")
            else:  # 1.x returns FetchedTranscriptSnippet objects
                text = getattr(entry, "text", None)
                start = getattr(entry, "start", None)
                dur = getattr(entry, "duration", None)
            if not text or start is None:
                continue
            cleaned = re.sub(r"\s+", " ", str(text).replace("\n", " ")).strip()
            if not cleaned or cleaned.startswith("[") and cleaned.endswith("]"):
                continue
            cues.append(TranscriptCue(float(start), float(dur or 0), cleaned))

        if len(cues) < 20:
            raise TranscriptError(
                f"Transcript for {video_id} is too short ({len(cues)} cues) to be useful."
            )
        log.info("Transcript OK for %s (%d cues, %.0f min).",
                 video_id, len(cues), cues[-1].end / 60)
        return cues

    # -- route 1: youtube-transcript-api -----------------------------------

    def _proxy_config(self):
        if not self.cfg.proxy_url:
            return None
        try:
            from youtube_transcript_api.proxies import GenericProxyConfig  # type: ignore

            return GenericProxyConfig(
                http_url=self.cfg.proxy_url, https_url=self.cfg.proxy_url
            )
        except ImportError:
            return None

    def _fetch_via_api(self, video_id: str, reasons: list[str]):
        try:
            from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore
        except ImportError:
            reasons.append("api:not-installed")
            return None

        raw = self._fetch_v1(YouTubeTranscriptApi, video_id, reasons)
        if raw is None:
            raw = self._fetch_v0(YouTubeTranscriptApi, video_id, reasons)
        return raw

    def _fetch_v1(self, api_cls, video_id: str, reasons: list[str]):
        if not hasattr(api_cls, "list") and not hasattr(api_cls, "fetch"):
            return None
        try:
            kwargs = {}
            proxy_cfg = self._proxy_config()
            if proxy_cfg is not None:
                kwargs["proxy_config"] = proxy_cfg
            instance = api_cls(**kwargs)
            return list(instance.fetch(video_id, languages=list(self.LANGS)))
        except TypeError:
            return None
        except Exception as exc:
            reasons.append(f"api:{type(exc).__name__}")
            log.warning("Transcript API failed for %s: %s: %s",
                        video_id, type(exc).__name__, str(exc)[:200])
            return None

    def _fetch_v0(self, api_cls, video_id: str, reasons: list[str]):
        getter = getattr(api_cls, "get_transcript", None)
        if getter is None:
            return None
        try:
            kwargs: dict[str, Any] = {"languages": list(self.LANGS)}
            if self.cfg.proxies:
                kwargs["proxies"] = self.cfg.proxies
            return getter(video_id, **kwargs)
        except Exception as exc:
            reasons.append(f"api0:{type(exc).__name__}")
            return None

    # -- route 2: yt-dlp ----------------------------------------------------

    def _fetch_via_ytdlp(self, video_id: str, reasons: list[str]):
        """Ask yt-dlp for the caption track URL, then parse it ourselves."""
        try:
            import yt_dlp  # type: ignore
        except ImportError:
            reasons.append("ytdlp:not-installed")
            return None

        opts: dict[str, Any] = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "socket_timeout": 45,
            "retries": 3,
            "geo_bypass": True,
            "extractor_args": {"youtube": {"player_client": ["web_safari", "web"]}},
        }
        if self.cookie_file:
            opts["cookiefile"] = str(self.cookie_file)
        if self.cfg.proxy_url:
            opts["proxy"] = self.cfg.proxy_url

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={video_id}", download=False
                )
        except Exception as exc:
            reasons.append(f"ytdlp:{type(exc).__name__}")
            log.warning("yt-dlp could not read %s: %s", video_id, str(exc)[:200])
            return None

        track = self._pick_track(info or {})
        if not track:
            reasons.append("ytdlp:no-captions")
            return None

        try:
            resp = requests.get(track["url"], timeout=60, proxies=self.cfg.proxies)
            resp.raise_for_status()
        except requests.RequestException as exc:
            reasons.append(f"ytdlp-fetch:{type(exc).__name__}")
            return None

        ext = track.get("ext", "")
        if ext == "json3":
            return self._parse_json3(resp.text)
        if ext in ("vtt", "srt"):
            return self._parse_vtt(resp.text)
        reasons.append(f"ytdlp:unsupported-format:{ext}")
        return None

    def _pick_track(self, info: dict[str, Any]) -> dict[str, Any] | None:
        """Prefer real subtitles over auto-generated, and json3 over vtt."""
        for source in ("subtitles", "automatic_captions"):
            available = info.get(source) or {}
            for lang in self.LANGS:
                tracks = available.get(lang)
                if not tracks:
                    continue
                for want in ("json3", "vtt", "srt"):
                    for track in tracks:
                        if track.get("ext") == want and track.get("url"):
                            log.info("Using %s captions (%s/%s).", source, lang, want)
                            return track
        return None

    @staticmethod
    def _parse_json3(body: str) -> list[dict[str, Any]]:
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return []
        out = []
        for event in data.get("events") or []:
            start = event.get("tStartMs")
            if start is None:
                continue
            text = "".join(seg.get("utf8", "") for seg in event.get("segs") or [])
            if not text.strip():
                continue
            out.append({
                "start": start / 1000.0,
                "duration": (event.get("dDurationMs") or 0) / 1000.0,
                "text": text,
            })
        return out

    @staticmethod
    def _parse_vtt(body: str) -> list[dict[str, Any]]:
        def to_seconds(stamp: str) -> float:
            stamp = stamp.replace(",", ".")
            bits = [float(b) for b in stamp.split(":")]
            while len(bits) < 3:
                bits.insert(0, 0.0)
            return bits[0] * 3600 + bits[1] * 60 + bits[2]

        out: list[dict[str, Any]] = []
        pattern = re.compile(
            r"(\d{1,2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[.,]\d{3})"
        )
        blocks = re.split(r"\n\s*\n", body)
        for block in blocks:
            match = pattern.search(block)
            if not match:
                continue
            start, end = to_seconds(match.group(1)), to_seconds(match.group(2))
            lines = block.split("\n")[match.string[: match.start()].count("\n") + 1 :]
            text = " ".join(lines)
            text = re.sub(r"<[^>]+>", "", text).strip()
            if text:
                out.append({"start": start, "duration": max(0.0, end - start), "text": text})

        # Auto-captions repeat each line as a rolling window - drop the dupes.
        deduped: list[dict[str, Any]] = []
        for cue in out:
            if deduped and cue["text"] == deduped[-1]["text"]:
                continue
            deduped.append(cue)
        return deduped


def cues_to_timestamped_text(cues: Iterable[TranscriptCue], max_chars: int = 45_000) -> str:
    """Flatten cues into '[123.4s] text' lines that Gemini can reason about."""
    lines: list[str] = []
    total = 0
    for cue in cues:
        line = f"[{cue.start:.1f}] {cue.text}"
        if total + len(line) > max_chars:
            break
        lines.append(line)
        total += len(line) + 1
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# STEP 2b - Gemini picks the viral segment
# ---------------------------------------------------------------------------


@dataclass
class ClipPlan:
    start: float
    end: float
    title: str
    description: str
    hashtags: list[str]
    reason: str = ""

    @property
    def duration(self) -> float:
        return self.end - self.start


PROMPT_TEMPLATE = """You are a world-class short-form video editor who has produced
viral YouTube Shorts in the technology and AI niche.

Below is the timestamped transcript of a long-form video. Each line is formatted
as `[seconds] spoken text`.

Your job: find the ONE continuous segment that would perform best as a standalone
vertical short.

Hard rules:
- The segment MUST be between {min_s} and {max_s} seconds long.
- It MUST be continuous (a single start and end, no jump cuts).
- It MUST start at a natural sentence beginning, not mid-word.
- It MUST make complete sense without any surrounding context.
- Prefer segments with: a surprising claim, a strong opinion, a concrete number
  or benchmark, a myth being busted, a clear "how to" nugget, or a bold prediction.
- Avoid: intros, sponsor reads, "like and subscribe", rambling, pure setup with
  no payoff, and anything where the speaker refers to something on screen that
  the viewer cannot see.

Then write the publishing metadata:
- `title`: max 80 characters, hooky, no clickbait lies, no emoji spam (1 emoji max).
- `description`: 1 to 2 short sentences describing the clip.
- `hashtags`: 4 to 6 relevant lowercase hashtags WITHOUT the '#' symbol.
- `reason`: one sentence on why this segment will hook viewers.

Return ONLY raw JSON, no markdown fences, in exactly this shape:
{{"start_seconds": 0.0, "end_seconds": 0.0, "title": "", "description": "",
  "hashtags": ["", ""], "reason": ""}}

VIDEO TITLE: {video_title}
CHANNEL: {channel}

TRANSCRIPT:
{transcript}
"""


class GeminiPlanner:
    BASE = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self._auth_mode: str | None = None

    def plan(self, cand: Candidate, cues: list[TranscriptCue]) -> ClipPlan:
        prompt = PROMPT_TEMPLATE.format(
            min_s=self.cfg.min_clip_seconds,
            max_s=self.cfg.max_clip_seconds,
            video_title=cand.title,
            channel=cand.channel,
            transcript=cues_to_timestamped_text(cues),
        )
        raw = self._generate(prompt)
        plan = self._parse(raw)
        return self._validate(plan, cues)

    # Google is midway through changing its API key format. Old keys start with
    # "AIza" and work as a ?key= query parameter. New keys start with "AQ." and
    # are rejected that way (401 ACCESS_TOKEN_TYPE_UNSUPPORTED) - they need a
    # header instead. We try each style until one works, then remember it.
    AUTH_MODES = ("header", "bearer", "query")

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
        return self.session.post(
            url, params=params, headers=headers, json=payload, timeout=120
        )

    @retry(times=4, delay=8.0, exceptions=(requests.RequestException, GeminiError))
    def _generate(self, prompt: str) -> str:
        url = f"{self.BASE}/{self.cfg.gemini_model}:generateContent"
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json",
            },
        }

        # If we already know which auth style this key likes, go straight to it.
        modes = (self._auth_mode,) if self._auth_mode else self.AUTH_MODES
        resp = None
        rejected: list[str] = []

        for mode in modes:
            resp = self._send(url, payload, mode)
            if resp.status_code in (401, 403) and not self._auth_mode:
                rejected.append(f"{mode}={resp.status_code}")
                log.info("Gemini rejected the %s auth style - trying the next one.", mode)
                continue
            if resp.status_code < 400:
                if self._auth_mode != mode:
                    log.info("Gemini accepted the '%s' auth style.", mode)
                self._auth_mode = mode
            break

        if resp is None:  # pragma: no cover - defensive
            raise GeminiError("Gemini request was never sent.")

        if resp.status_code in (401, 403):
            # A rejected key is never a temporary glitch, so raise ConfigError
            # rather than GeminiError: that skips the retries AND stops the whole
            # run instead of burning through every candidate with a dead key.
            raise ConfigError(
                f"Gemini rejected your API key with every auth style ({', '.join(rejected)}). "
                f"Key starts with '{self.cfg.gemini_api_key[:4]}'. "
                "If it starts with 'AQ.', create a replacement key from the Google Cloud "
                "Console instead of AI Studio (see the README) - those come out in the "
                f"older 'AIza' format. Server said: {resp.text[:300]}"
            )
        if resp.status_code == 429:
            raise GeminiError("Gemini free-tier rate limit hit (429).")
        if resp.status_code >= 400:
            raise GeminiError(f"Gemini HTTP {resp.status_code}: {resp.text[:400]}")

        data = resp.json()
        candidates = data.get("candidates") or []
        if not candidates:
            feedback = data.get("promptFeedback", {})
            raise GeminiError(f"Gemini returned no candidates. Feedback: {feedback}")
        parts = (candidates[0].get("content") or {}).get("parts") or []
        text = "".join(p.get("text", "") for p in parts).strip()
        if not text:
            raise GeminiError("Gemini returned an empty response body.")
        return text

    @staticmethod
    def _parse(raw: str) -> ClipPlan:
        cleaned = raw.strip()
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.MULTILINE).strip()
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            raise GeminiError(f"No JSON object found in Gemini output: {raw[:300]}")
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError as exc:
            raise GeminiError(f"Gemini JSON was malformed: {exc}") from exc

        try:
            start = float(obj["start_seconds"])
            end = float(obj["end_seconds"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GeminiError(f"Gemini JSON missing usable timestamps: {obj}") from exc

        tags = obj.get("hashtags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]
        tags = [re.sub(r"[^a-z0-9]", "", str(t).lower()) for t in tags]
        tags = [t for t in tags if t][:6]

        return ClipPlan(
            start=start,
            end=end,
            title=str(obj.get("title") or "").strip()[:95] or "AI Insight",
            description=str(obj.get("description") or "").strip()[:400],
            hashtags=tags or ["ai", "tech", "shorts"],
            reason=str(obj.get("reason") or "").strip()[:300],
        )

    def _validate(self, plan: ClipPlan, cues: list[TranscriptCue]) -> ClipPlan:
        transcript_end = cues[-1].end if cues else 0.0
        if plan.start < 0:
            plan.start = 0.0
        if plan.end <= plan.start:
            raise GeminiError(f"Gemini gave an inverted range: {plan.start} -> {plan.end}")

        # Clamp the duration into our allowed window instead of failing outright.
        duration = plan.duration
        if duration < self.cfg.min_clip_seconds:
            log.info("Clip was %.1fs - extending to %ds.", duration, self.cfg.min_clip_seconds)
            plan.end = plan.start + self.cfg.min_clip_seconds
        elif duration > self.cfg.max_clip_seconds:
            log.info("Clip was %.1fs - trimming to %ds.", duration, self.cfg.max_clip_seconds)
            plan.end = plan.start + self.cfg.max_clip_seconds

        if transcript_end and plan.end > transcript_end + 5:
            shift = plan.end - transcript_end
            plan.start = max(0.0, plan.start - shift)
            plan.end = max(plan.start + self.cfg.min_clip_seconds, transcript_end)
            log.info("Clip ran past the transcript - shifted back to %.1f-%.1f.",
                     plan.start, plan.end)

        log.info(
            "Gemini picked %s -> %s (%.1fs) | %s",
            hhmmss(plan.start), hhmmss(plan.end), plan.duration, plan.title,
        )
        if plan.reason:
            log.info("Reason: %s", plan.reason)
        return plan


# ---------------------------------------------------------------------------
# STEP 3 - Download just the segment with yt-dlp
# ---------------------------------------------------------------------------


def write_cookie_file(cfg: Config, workdir: Path) -> Path | None:
    """Turn the YT_COOKIES secret into a file yt-dlp can read. Shared by the
    transcript fetcher and the downloader so both get the same session."""
    if not cfg.yt_cookies:
        return None
    path = workdir / "cookies.txt"
    # GitHub Secrets sometimes collapse literal \n - repair them.
    content = cfg.yt_cookies.replace("\\n", "\n")
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o600)
    log.info("Using supplied YouTube cookies.")
    return path


class SegmentDownloader:
    def __init__(self, cfg: Config, workdir: Path, cookie_file: Path | None = None) -> None:
        self.cfg = cfg
        self.workdir = workdir
        self.cookie_file = cookie_file

    def download(self, cand: Candidate, plan: ClipPlan) -> Path:
        try:
            import yt_dlp  # type: ignore
            from yt_dlp.utils import download_range_func  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise DownloadError("yt-dlp is not installed.") from exc

        out_template = str(self.workdir / "raw.%(ext)s")
        # A little padding so FFmpeg keyframe snapping never clips the first word.
        start = max(0.0, plan.start - 0.3)
        end = plan.end + 0.3

        opts: dict[str, Any] = {
            "format": (
                "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best"
            ),
            "merge_output_format": "mp4",
            "outtmpl": out_template,
            "download_ranges": download_range_func(None, [(start, end)]),
            "force_keyframes_at_cuts": True,
            "noprogress": True,
            "quiet": True,
            "no_warnings": True,
            "retries": 5,
            "fragment_retries": 5,
            "socket_timeout": 45,
            "concurrent_fragment_downloads": 4,
            "geo_bypass": True,
            # Web client is the least likely to trip "confirm you're not a bot".
            "extractor_args": {"youtube": {"player_client": ["web_safari", "web"]}},
        }
        if self.cookie_file:
            opts["cookiefile"] = str(self.cookie_file)
        if self.cfg.proxy_url:
            opts["proxy"] = self.cfg.proxy_url

        log.info("Downloading %s from %s to %s ...", cand.video_id, hhmmss(start), hhmmss(end))
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([cand.url])
        except Exception as exc:
            raise DownloadError(
                f"yt-dlp failed for {cand.video_id}: {exc}. "
                "If this says 'Sign in to confirm you're not a bot', add the "
                "YT_COOKIES and/or PROXY_URL secrets."
            ) from exc

        produced = sorted(
            (p for p in self.workdir.glob("raw.*") if p.suffix.lower() != ".part"),
            key=lambda p: p.stat().st_size,
            reverse=True,
        )
        if not produced or produced[0].stat().st_size < 50_000:
            raise DownloadError("yt-dlp produced no usable file (or an empty one).")
        log.info("Downloaded %s (%.1f MB).", produced[0].name,
                 produced[0].stat().st_size / 1_048_576)
        return produced[0]


# ---------------------------------------------------------------------------
# STEP 3 (cont.) - Reframe to 9:16 with FFmpeg
# ---------------------------------------------------------------------------


class VerticalRenderer:
    WIDTH, HEIGHT = 1080, 1920

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ffmpeg = shutil.which("ffmpeg")
        self.ffprobe = shutil.which("ffprobe")
        if not self.ffmpeg:
            raise RenderError(
                "ffmpeg was not found on PATH. GitHub's ubuntu-latest runner ships "
                "with it; add an apt-get install step if you changed the runner."
            )

    def probe_duration(self, path: Path) -> float:
        if not self.ffprobe:
            return 0.0
        try:
            out = subprocess.run(
                [self.ffprobe, "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=nw=1:nk=1", str(path)],
                capture_output=True, text=True, timeout=60, check=True,
            )
            return float(out.stdout.strip() or 0)
        except (subprocess.SubprocessError, ValueError):
            return 0.0

    def _filter_chain(self) -> str:
        if self.cfg.crop_mode == "center":
            # Hard centre crop: fills the frame, loses the left/right edges.
            return (
                f"scale={self.WIDTH}:{self.HEIGHT}:force_original_aspect_ratio=increase,"
                f"crop={self.WIDTH}:{self.HEIGHT},setsar=1"
            )
        # Blurred-background composite: keeps the whole frame, fills the gaps.
        return (
            "[0:v]split=2[bg][fg];"
            f"[bg]scale={self.WIDTH}:{self.HEIGHT}:force_original_aspect_ratio=increase,"
            f"crop={self.WIDTH}:{self.HEIGHT},gblur=sigma=25,eq=brightness=-0.08[bgb];"
            f"[fg]scale={self.WIDTH}:{self.HEIGHT}:force_original_aspect_ratio=decrease[fgs];"
            "[bgb][fgs]overlay=(W-w)/2:(H-h)/2,setsar=1[v]"
        )

    def render(self, source: Path, dest: Path, plan: ClipPlan) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        chain = self._filter_chain()

        cmd = [self.ffmpeg, "-hide_banner", "-loglevel", "error", "-y", "-i", str(source)]
        if self.cfg.crop_mode == "center":
            cmd += ["-vf", chain]
        else:
            cmd += ["-filter_complex", chain, "-map", "[v]", "-map", "0:a?"]
        cmd += [
            "-t", f"{min(plan.duration, self.cfg.max_clip_seconds):.2f}",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "21",
            "-profile:v", "high",
            "-pix_fmt", "yuv420p",
            "-r", "30",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "160k",
            "-ar", "44100",
            "-ac", "2",
            "-movflags", "+faststart",
            str(dest),
        ]

        log.info("Rendering 9:16 vertical (%s mode) ...", self.cfg.crop_mode)
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        except subprocess.TimeoutExpired as exc:
            raise RenderError("FFmpeg timed out after 15 minutes.") from exc

        if proc.returncode != 0:
            raise RenderError(f"FFmpeg exited {proc.returncode}: {proc.stderr[-1200:]}")
        if not dest.exists() or dest.stat().st_size < 20_000:
            raise RenderError("FFmpeg produced no output (or a suspiciously tiny file).")

        duration = self.probe_duration(dest)
        if duration and duration > 61:
            raise RenderError(
                f"Rendered clip is {duration:.1f}s - YouTube will not treat it as a Short."
            )
        log.info("Rendered %s (%.1f MB, %.1fs).", dest.name,
                 dest.stat().st_size / 1_048_576, duration)
        return dest


# ---------------------------------------------------------------------------
# STEP 4 - Upload to YouTube as a Short
# ---------------------------------------------------------------------------


class YouTubeUploader:
    SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def _credentials(self):
        try:
            from google.oauth2.credentials import Credentials  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise UploadError("google-auth is not installed.") from exc

        return Credentials(
            token=None,
            refresh_token=self.cfg.yt_refresh_token,
            client_id=self.cfg.yt_client_id,
            client_secret=self.cfg.yt_client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=self.SCOPES,
        )

    def upload(self, video: Path, plan: ClipPlan, cand: Candidate) -> str:
        try:
            from googleapiclient.discovery import build  # type: ignore
            from googleapiclient.errors import HttpError  # type: ignore
            from googleapiclient.http import MediaFileUpload  # type: ignore
            from google.auth.transport.requests import Request  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise UploadError("google-api-python-client is not installed.") from exc

        creds = self._credentials()
        try:
            creds.refresh(Request())
        except Exception as exc:
            raise UploadError(
                f"Could not refresh the YouTube OAuth token: {exc}. "
                "Re-generate YT_REFRESH_TOKEN (see README step 4)."
            ) from exc

        title = self._build_title(plan)
        body = {
            "snippet": {
                "title": title,
                "description": self._build_description(plan, cand),
                "tags": (plan.hashtags + ["shorts", "ai", "technology"])[:15],
                "categoryId": "28",  # Science & Technology
            },
            "status": {
                "privacyStatus": self.cfg.upload_privacy,
                "selfDeclaredMadeForKids": False,
                "license": "creativeCommon",
            },
        }

        youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)
        media = MediaFileUpload(str(video), chunksize=4 * 1024 * 1024, resumable=True,
                                mimetype="video/mp4")
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

        log.info("Uploading to YouTube as %s ...", self.cfg.upload_privacy)
        response = None
        errors = 0
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
        log.info("YouTube upload complete: https://youtube.com/shorts/%s", video_id)
        return video_id

    @staticmethod
    def _build_title(plan: ClipPlan) -> str:
        title = plan.title.strip()
        if "#shorts" not in title.lower():
            title = f"{title} #Shorts"
        return title[:100]

    @staticmethod
    def _build_description(plan: ClipPlan, cand: Candidate) -> str:
        tags = " ".join(f"#{t}" for t in plan.hashtags)
        return (
            f"{plan.description}\n\n"
            f"{tags} #Shorts\n\n"
            "-----\n"
            "ATTRIBUTION\n"
            f"{cand.attribution}\n"
            "This clip is an excerpt reused under the terms of that licence."
        )[:4900]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def build_clip(cfg: Config, workdir: Path, state: State) -> tuple[Path, ClipPlan, Candidate]:
    """Walk candidates until one of them yields a finished vertical clip."""
    cookie_file = write_cookie_file(cfg, workdir)
    source = YouTubeSource(cfg)
    transcripts = TranscriptFetcher(cfg, cookie_file)
    planner = GeminiPlanner(cfg)
    downloader = SegmentDownloader(cfg, workdir, cookie_file)
    renderer = VerticalRenderer(cfg)

    candidates = source.find_candidates()
    fresh = [c for c in candidates if not state.seen(c.video_id)]
    if not fresh:
        raise NoCandidateError(
            "Every candidate found today has already been used. "
            "Widen SEARCH_QUERIES or raise MAX_CANDIDATES."
        )

    last_error: Exception | None = None
    tally: dict[str, int] = {}
    for index, cand in enumerate(fresh, start=1):
        log.info("-" * 70)
        log.info("Candidate %d/%d: %s - %s (%s, %s views)",
                 index, len(fresh), cand.channel, cand.title,
                 hhmmss(cand.duration_s), f"{cand.view_count:,}")
        try:
            cues = transcripts.fetch(cand.video_id)
            plan = planner.plan(cand, cues)
            raw = downloader.download(cand, plan)
            dest = cfg.output_dir / f"{sanitize_filename(plan.title)}_{cand.video_id}.mp4"
            final = renderer.render(raw, dest, plan)
            state.mark(cand.video_id)
            return final, plan, cand
        except (TranscriptError, GeminiError, DownloadError, RenderError) as exc:
            label = type(exc).__name__.replace("Error", "").lower()
            tally[label] = tally.get(label, 0) + 1
            log.warning("Candidate %s skipped (%s): %s", cand.video_id, label, exc)
            last_error = exc

            # Only blacklist a video when the problem is permanent (it genuinely
            # has no captions). A network block or rate limit is temporary, and
            # blacklisting on those would silently burn the whole candidate pool
            # during an outage.
            permanent = True
            if isinstance(exc, TranscriptError):
                permanent = "no-captions" in transcripts.last_reason
            if permanent:
                state.mark(cand.video_id)
            else:
                log.info("Not blacklisting %s - looks like a temporary block.",
                         cand.video_id)

            for leftover in workdir.glob("raw.*"):
                leftover.unlink(missing_ok=True)
            continue

    breakdown = ", ".join(f"{n}x {k}" for k, n in sorted(tally.items()))
    hint = ""
    if tally.get("transcript", 0) >= max(3, len(fresh) // 2):
        hint = (
            " Most failures were transcript fetches, which usually means YouTube is "
            "blocking this datacenter IP. Adding the YT_COOKIES secret normally fixes it "
            "- see the README."
        )
    raise NoCandidateError(
        f"All {len(fresh)} candidates failed ({breakdown}). Last error: {last_error}.{hint}"
    )


def distribute(cfg: Config, video: Path, plan: ClipPlan, cand: Candidate) -> dict[str, Any]:
    results: dict[str, Any] = {"youtube": None, "errors": []}

    try:
        results["youtube"] = YouTubeUploader(cfg).upload(video, plan, cand)
    except UploadError as exc:
        log.error("YouTube upload failed: %s", exc)
        results["errors"].append(f"youtube: {exc}")

    return results


def write_summary(payload: dict[str, Any]) -> None:
    """Write a human-readable run summary into the GitHub Actions job page."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("## Shorts Factory run\n\n")
            for key, value in payload.items():
                fh.write(f"- **{key}**: {value}\n")
            fh.write("\n")
    except OSError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Automated faceless Shorts factory.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build the clip but do not upload anywhere.")
    parser.add_argument("--keep-temp", action="store_true",
                        help="Do not delete the temporary working directory.")
    args = parser.parse_args()

    start_time = time.time()
    try:
        cfg = Config.from_env(dry_run=args.dry_run)
    except ConfigError as exc:
        log.error("%s", exc)
        return 2

    slot = os.environ.get("POST_SLOT", "manual")
    log.info("=== Shorts Factory | posting slot: %s ===", slot)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    state = State(cfg.state_file)
    workdir = Path(tempfile.mkdtemp(prefix="shorts_"))
    log.info("Working directory: %s", workdir)

    exit_code = 0
    try:
        video, plan, cand = build_clip(cfg, workdir, state)

        if args.dry_run:
            log.info("DRY RUN - skipping uploads. Clip is at %s", video)
            write_summary(
                {"slot": slot, "mode": "dry-run", "clip": str(video), "title": plan.title}
            )
        else:
            results = distribute(cfg, video, plan, cand)
            write_summary(
                {
                    "slot": slot,
                    "source": f"{cand.title} ({cand.channel})",
                    "segment": f"{hhmmss(plan.start)} - {hhmmss(plan.end)}",
                    "title": plan.title,
                    "youtube": (
                        f"https://youtube.com/shorts/{results['youtube']}"
                        if results["youtube"] else "not uploaded"
                    ),
                    "errors": "; ".join(results["errors"]) or "none",
                }
            )
            if results["errors"] and not results["youtube"]:
                exit_code = 1

    except NoCandidateError as exc:
        log.warning("Nothing to post today: %s", exc)
        write_summary({"result": "no candidate", "detail": str(exc)})
        exit_code = 0  # an empty day is not a build failure
    except ConfigError as exc:
        log.error("Configuration problem: %s", exc)
        exit_code = 2
    except PipelineError as exc:
        log.error("Pipeline failure: %s", exc)
        write_summary({"result": "failed", "detail": str(exc)})
        exit_code = 1
    except Exception as exc:  # last-resort net so state still gets saved
        log.exception("Unexpected error: %s", exc)
        write_summary({"result": "crashed", "detail": str(exc)})
        exit_code = 1
    finally:
        state.save()
        if not args.keep_temp:
            shutil.rmtree(workdir, ignore_errors=True)
        log.info("Finished in %.1fs (exit=%d).", time.time() - start_time, exit_code)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
