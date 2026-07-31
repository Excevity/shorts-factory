# Faceless Shorts Factory

A fully automated YouTube Shorts channel that runs itself inside GitHub Actions. No software to install, no local models, no compute bills.

**Three times a day — 10:00 AM, 12:00 PM and 2:00 PM New York time** — GitHub rents you a free Linux computer for a few minutes. That computer finds a Creative Commons tech/AI video, asks Google Gemini which 45-60 seconds of it are the most interesting, cuts exactly that piece out, reshapes it into a vertical video, uploads it to your YouTube channel as a Short, and then shuts itself down.

Each of the three runs picks a *different* source video — they share a memory file (`state/processed.json`) so nothing gets posted twice.

---

## What each file does

| File | What it is |
|---|---|
| `main.py` | The whole pipeline. Search → transcript → Gemini → download → FFmpeg → upload. |
| `requirements.txt` | The list of Python libraries the runner should install. |
| `.github/workflows/automate.yml` | The alarm clock. Tells GitHub when and how to run `main.py` (3× daily). |
| `state/processed.json` | A shared memory file so the same source video never gets used twice. |

---

## Some words you'll see, explained

- **Repository (repo)** — a folder on GitHub that holds your project files.
- **GitHub Actions** — GitHub's free robot. It can run your code on a schedule on a rented Linux machine. Public repos get unlimited free minutes.
- **Runner** — the temporary Linux computer Actions rents you. It's wiped clean after every run.
- **Secret** — an encrypted value (like an API key) you store in GitHub. Your code can read it, but nobody can see it, not even you after saving it.
- **API key** — a password that lets your code talk to a service like YouTube or Gemini.
- **OAuth refresh token** — a special long-lived key that lets the robot *act as you* on YouTube (i.e. upload to your channel). An API key alone can only read; uploading needs this.
- **Cron** — the format used to say "run at this time". `0 17 * * *` means "at 17:00 UTC every day".
- **CC-BY / Creative Commons Attribution** — a licence where the creator says "reuse my video, just credit me." The script auto-adds the credit to every description.

---

# Setup — do these in order

Budget about 40 minutes the first time. Steps 1-3 are quick. Step 4 is the fiddly one.

## Step 1 — Create the GitHub repository

1. Go to <https://github.com> and sign in (create a free account if you don't have one).
2. Click the **+** in the top-right corner → **New repository**.
3. Repository name: `shorts-factory`
4. Choose **Public**.
   > **Why public?** Public repos get unlimited free Actions minutes. Private repos only get 2,000 minutes/month. Nothing sensitive is ever stored in the code — your keys live in Secrets, which stay encrypted even on a public repo.
5. Tick **Add a README file**.
6. Click **Create repository**.

## Step 2 — Upload the files

The tricky part is that two of the files live inside folders. GitHub's web uploader can create folders for you if you type the path.

**2a. Upload `main.py` and `requirements.txt`**

1. On your repo page, click **Add file** → **Upload files**.
2. Drag `main.py` and `requirements.txt` into the box.
3. Click **Commit changes**.

**2b. Create the workflow file**

1. Click **Add file** → **Create new file**.
2. In the filename box, type exactly: `.github/workflows/automate.yml`
   *(As you type each `/`, GitHub turns it into a folder — that's correct.)*
3. Open `automate.yml` on your computer in Notepad, select all, copy, and paste it into the big editor box.
4. Click **Commit changes**.

**2c. Create the state file**

1. Click **Add file** → **Create new file**.
2. Filename: `state/processed.json`
3. Paste in exactly:
   ```json
   {"updated_at": null, "processed_video_ids": []}
   ```
4. Click **Commit changes**.

Your repo should now look like this:

```
shorts-factory/
├── .github/
│   └── workflows/
│       └── automate.yml
├── state/
│   └── processed.json
├── main.py
├── requirements.txt
└── README.md
```

## Step 3 — Get your API keys

You need **five** required values. Keep them in a scratch text file for now; you'll paste them into GitHub in Step 5, then delete the scratch file.

### 3a. Google Cloud project (needed for both YouTube keys)

1. Go to <https://console.cloud.google.com>.
2. Top bar → project dropdown → **New Project**. Name it `shorts-factory`. Create.
3. Make sure the new project is selected in that dropdown.
4. Go to **APIs & Services** → **Library**. Search **YouTube Data API v3** → click it → **Enable**.

### 3b. `YOUTUBE_API_KEY` (for searching)

1. **APIs & Services** → **Credentials** → **+ Create Credentials** → **API key**.
2. Copy the key. That's `YOUTUBE_API_KEY`.

### 3c. `YT_CLIENT_ID` and `YT_CLIENT_SECRET` (for uploading)

1. **APIs & Services** → **OAuth consent screen**.
   - User type: **External** → Create.
   - App name: `shorts-factory`. Support email + developer email: your own. Save and continue.
   - **Scopes** page: click **Add or remove scopes**, search `youtube.upload`, tick `.../auth/youtube.upload`, then **Update** → **Save and continue**.
   - **Test users** page: click **+ Add users**, add your own Gmail address. Save and continue.
   - Leave the app in **Testing** mode. That's fine — it just means the token expires every 7 days unless you publish. See the note at the end of this step.
2. **Credentials** → **+ Create Credentials** → **OAuth client ID**.
   - Application type: **Web application**.
   - Name: `shorts-factory-client`.
   - Under **Authorised redirect URIs**, click **+ Add URI** and paste exactly:
     `https://developers.google.com/oauthplayground`
   - Click **Create**.
3. A popup shows your **Client ID** and **Client Secret**. Copy both. These are `YT_CLIENT_ID` and `YT_CLIENT_SECRET`.

> **Important:** while your OAuth app is in "Testing" mode, Google expires the refresh token after 7 days and you'd have to redo Step 4 weekly. To avoid that, go back to **OAuth consent screen** and click **Publish app**. It will say "needs verification" — ignore that. Verification is only required for apps used by *other* people. For your own account, publishing is enough to make the token permanent.

### 3d. `GEMINI_API_KEY`

1. Go to <https://aistudio.google.com/app/apikey>.
2. Sign in → **Create API key** → pick your `shorts-factory` project.
3. Copy it. That's `GEMINI_API_KEY`. The free tier is generous — this script uses roughly one request per day.

## Step 4 — Get `YT_REFRESH_TOKEN` (the fiddly one)

This is the key that lets the robot upload to *your* channel. You get it once, in your browser, with no software.

1. Go to <https://developers.google.com/oauthplayground>.
2. Click the **gear icon** (⚙️) in the top-right.
3. Tick **Use your own OAuth credentials**.
4. Paste your `YT_CLIENT_ID` into **OAuth Client ID** and `YT_CLIENT_SECRET` into **OAuth Client secret**. Close the gear panel.
5. In the left column **Step 1**, find the box labelled *"Input your own scopes"* at the bottom and paste:
   ```
   https://www.googleapis.com/auth/youtube.upload
   ```
6. Click **Authorize APIs**.
7. Sign in with the Google account that owns your YouTube channel. You'll see a scary "Google hasn't verified this app" screen — click **Advanced** → **Go to shorts-factory (unsafe)**. This is your own app, so it's fine. Then **Continue**/**Allow**.
8. You land back on the playground at **Step 2**. Click **Exchange authorization code for tokens**.
9. A **Refresh token** appears, starting with `1//`. Copy the whole thing. That's `YT_REFRESH_TOKEN`.

> If no refresh token appears, click the gear again, make sure **Force prompt consent** is ticked, and redo steps 6-8.

## Step 5 — Paste your keys into GitHub Secrets

**This is the exact place your keys go.**

1. Open your repo on GitHub.
2. Click the **Settings** tab (top of the repo, far right — *not* your account settings).
3. In the left sidebar, expand **Secrets and variables** → click **Actions**.
4. Make sure you're on the **Repository secrets** tab.
5. Click **New repository secret** for each row below. The **Name** must match *exactly* — capital letters and underscores included.

| Name (paste exactly) | Value | Required? |
|---|---|---|
| `YOUTUBE_API_KEY` | The API key from 3b | ✅ Yes |
| `GEMINI_API_KEY` | The key from 3d | ✅ Yes |
| `YT_CLIENT_ID` | Ends in `.apps.googleusercontent.com` | ✅ Yes |
| `YT_CLIENT_SECRET` | Usually starts `GOCSPX-` | ✅ Yes |
| `YT_REFRESH_TOKEN` | Starts with `1//` | ✅ Yes |
| `YT_COOKIES` | See Step 7 | ⬜ Optional |
| `PROXY_URL` | See Step 7 | ⬜ Optional |

6. After saving, GitHub shows names only, never values. If you mistype a value later, use **Update** to overwrite it — you can't read it back.

You can delete your scratch text file now.

## Step 6 — Test it

1. Go to the **Actions** tab in your repo.
2. If you see a "Workflows aren't being run on this forked repository" or enable prompt, click the green **I understand my workflows, go ahead and enable them**.
3. Click **Daily Shorts Factory** in the left sidebar.
4. Click **Run workflow** (top right) → tick **dry run** → **Run workflow**.
   - A dry run builds the video but uploads nothing. Perfect for the first test.
5. Wait 3-8 minutes. Click into the run to watch the live log.
6. When it's green, scroll to the bottom of the run page and download the **short-manual-run1** artifact. Unzip it and watch the MP4.
7. Happy with it? Run it again with **dry run unticked**. The Short lands on your channel as **Private** — go to YouTube Studio, review it, and flip it to Public yourself.

From then on it runs by itself at 10 AM, 12 PM and 2 PM New York time. Each run posts one Short, so that's three a day.

> **Heads-up on the schedule:** GitHub's free scheduler is best-effort, not precise. Runs often start 5-20 minutes late, and during very busy periods a run can be dropped entirely. That's normal and not something you can fix — it just means some days you'll get two Shorts instead of three.

Once you trust it, change `UPLOAD_PRIVACY: "private"` to `"public"` inside `automate.yml` to go fully hands-off. But read the next section first — there's a Google restriction that will bite you here.

---

## Before you switch to public: the API audit

This one catches everybody, so it's worth knowing up front.

Google restricts videos uploaded through an **unaudited** API project. If your Google Cloud project was created after 28 July 2020 (yours was), **every video uploaded via the API is locked to private** — and you can't flip it to public in YouTube Studio. YouTube emails the channel owner explaining why.

So setting `UPLOAD_PRIVACY: "public"` won't do anything until you fix this. Two options:

- **Live with it.** Leave it on `private`, and manually publish the ones you like from YouTube Studio. Totally viable, and honestly a good habit anyway — 30 seconds of review per clip.
- **Request the audit.** Fill in Google's YouTube API Services compliance audit form (linked from the [videos.insert docs](https://developers.google.com/youtube/v3/docs/videos/insert)). It's free. They'll ask what your app does and how it uses the data. Turnaround varies and approval isn't guaranteed for automated repost tools, so don't count on it.

**Quota check:** three uploads a day is comfortably within the free tier. You get 10,000 quota units daily; each upload costs 1,600 and each search costs 100. Three runs works out to roughly 5,000-6,000 units. Going much past **five uploads a day would exceed the free quota**, so treat that as your ceiling unless you request more.

---

## Step 7 — If YouTube starts blocking the runner (optional)

GitHub's runners use datacenter IP addresses, and YouTube sometimes shows those "Sign in to confirm you're not a bot" checks. If your logs show that message, you have two fixes:

**Fix A — cookies (free)**

1. Install a browser extension that exports cookies in **Netscape format** (e.g. "Get cookies.txt LOCALLY").
2. Open YouTube while logged into a **throwaway Google account** (not your main one — cookies grant account access).
3. Export the cookies file, open it in Notepad, copy everything.
4. Save it as a GitHub Secret named `YT_COOKIES`.

**Fix B — a proxy (usually paid)**

Save a residential proxy URL as `PROXY_URL`, formatted like `http://user:pass@host:port`. The script passes it to both yt-dlp and the transcript fetcher.

The script works fine without either. They're only there for when you need them.

---

## Tuning the channel

Open `.github/workflows/automate.yml` and edit the values under `# tuning knobs`. These aren't secrets, so they live in plain text:

| Setting | Meaning |
|---|---|
| `SEARCH_QUERIES` | Comma-separated topics to hunt for. Change these to change your niche. |
| `DAYS_BACK` | How recent the source video must be. Set to 60 so the pool is big enough for 3 posts a day. |
| `MAX_CANDIDATES` | How many source videos to try before giving up for that run. |
| `MIN_CLIP_SECONDS` / `MAX_CLIP_SECONDS` | Clip length window. Keep max at 59 — 60+ stops being a Short. |
| `CROP_MODE` | `blur` = whole frame kept, blurred background fills the gaps. `center` = hard crop, fills the screen but cuts the sides off. |
| `UPLOAD_PRIVACY` | `private`, `unlisted`, or `public`. |
| `GEMINI_MODEL` | Swap to a newer Gemini model when one ships. |

### Changing the posting times

GitHub's scheduler only speaks UTC, and New York shifts by an hour twice a year for daylight saving. So the workflow wakes up **six** times a day (14:00-19:00 UTC) and the *Check New York local time* step immediately cancels the three that aren't a real posting slot. Net result: exactly three runs a day, at the right local time, all year round.

To change the times, edit the `case` block in that step:

```yaml
case "$HOUR" in
  10) ... slot=10am ;;
  12) ... slot=12pm ;;
  14) ... slot=2pm  ;;
```

Those numbers are New York hours on a 24-hour clock (so 5 PM would be `17`). If you pick hours outside the current 10-14 window, widen the `cron:` line to cover them — the rule is *(your hour + 4)* and *(your hour + 5)* in UTC.

**To go back to fewer posts a day**, just delete the lines you don't want from the `case` block. To add a fourth, add another line — but check the quota note above first.

---

## What happens when things go wrong

The script is built to fail softly rather than crash:

- **No transcript / Gemini fails / download blocked?** It logs the reason, marks that source video as used, and moves to the next candidate automatically.
- **All candidates failed?** It exits with code 0 (a green run) and writes "no candidate" in the run summary. A missed slot isn't a build failure — the next one tries again in two hours.
- **Two runs picked the same video?** They can't. Each run pulls the newest `state/processed.json` before starting and commits it back afterwards, and they're spaced two hours apart so there's no race.
- **The upload itself failed?** The rendered MP4 is still saved as a downloadable artifact, so no work is lost — and the exact error is written into the run summary.
- **A required secret is missing?** It exits with code 2 and names the exact secret.

Every run writes a summary at the bottom of the Actions run page showing which slot it was (10am / 12pm / 2pm), the source video, the chosen timestamps, the title, and the resulting link.

---

## Legal note

The script only searches with `videoLicense=creativeCommon` and double-checks the licence on a second API call, so it should only ever touch CC-BY material. It auto-writes the required attribution (original title, channel, URL, licence link) into every YouTube description.

That said, YouTube creators sometimes mislabel their videos as CC when the content isn't theirs to license. Keeping `UPLOAD_PRIVACY` on `private` and eyeballing each clip before publishing is the safe habit — it takes ten seconds and protects your channel from a copyright strike.

I'm not a lawyer and this isn't legal advice; if you plan to monetise the channel, read YouTube's reused-content policy first, since compilations of other people's clips with no added commentary can fail monetisation review.
