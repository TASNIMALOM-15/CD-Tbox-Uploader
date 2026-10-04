import asyncio
import json
import logging
import os
import re
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from terabox import BLOCK, BlockSource, FileSource, TeraBox, TeraBoxError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")

# ───────────────────────── কনফিগ (Environment Variables) ─────────────────────────
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_IDS = {int(x) for x in os.getenv("OWNER_IDS", "").replace(" ", "").split(",") if x}
if not OWNER_IDS:
    raise SystemExit("OWNER_IDS সেট করুন (আপনার টেলিগ্রাম user id), নইলে যে কেউ আপনার বট ব্যবহার করবে।")

TERABOX_COOKIE = os.getenv("TERABOX_NDUS", "")
TERABOX_DIR = os.getenv("TERABOX_DIR", "/TGBot")
DEFAULT_MODE = os.getenv("DEFAULT_MODE", "upload")  # upload | compress | cupload
WORKDIR = Path(os.getenv("WORKDIR", "/tmp/work"))
PORT = int(os.getenv("PORT", "10000"))
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "50"))

# FFmpeg সেটিংস — Render ফ্রি (0.1 CPU) এ ধীর হবে, তাই রাতারাতি চালানোর জন্য ডিজাইন করা
X264_PRESET = os.getenv("X264_PRESET", "veryfast")  # খুব দুর্বল CPU তে 'ultrafast' (ফাইল বড় হবে)
CRF = os.getenv("CRF", "28")
MAX_HEIGHT = os.getenv("MAX_HEIGHT", "720")
AUDIO_BR = os.getenv("AUDIO_BR", "96k")
FF_THREADS = os.getenv("FF_THREADS", "1")
TG_LIMIT = 2000 * 1024 * 1024  # টেলিগ্রাম বট আপলোড সীমা (নিরাপদ মার্জিন সহ)

MODES = {
    "upload": "☁️ TeraBox (অরিজিনাল, ডিস্ক-ছাড়া স্ট্রিম)",
    "compress": "🗜 কমপ্রেস → টেলিগ্রামে ফেরত",
    "cupload": "🗜☁️ কমপ্রেস → TeraBox",
}

app = Client("tgbox", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, workers=4, max_concurrent_transmissions=1)
tb = TeraBox(TERABOX_COOKIE, TERABOX_DIR) if TERABOX_COOKIE else None


# ───────────────────────── স্টেট ─────────────────────────
class Cancelled(Exception):
    pass


@dataclass
class Job:
    chat_id: int
    msg_id: int
    mode: str
    name: str
    size: int


queue: "asyncio.Queue[Job]" = asyncio.Queue()
pending: list[Job] = []
current: Optional[Job] = None
cancel = asyncio.Event()
proc: Optional[asyncio.subprocess.Process] = None
modes: dict[int, str] = {}
STATE_FILE = WORKDIR / "state.json"


def save_state():
    try:
        jobs = ([current] if current else []) + pending
        STATE_FILE.write_text(json.dumps({"jobs": [asdict(j) for j in jobs], "modes": modes}))
    except Exception:
        log.exception("state save failed")


def load_state() -> list[Job]:
    try:
        d = json.loads(STATE_FILE.read_text())
        modes.update({int(k): v for k, v in d.get("modes", {}).items()})
        return [Job(**j) for j in d.get("jobs", [])]
    except Exception:
        return []


# ───────────────────────── হেল্পার ─────────────────────────
def hsize(n: float) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"


def hdur(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h {s % 3600 // 60}m" if s >= 3600 else f"{s // 60}m {s % 60}s"


def safe_name(n: str) -> str:
    return re.sub(r'[\\/:*?"<>|\r\n]+', "_", n).strip()[:150] or "video.mp4"


def bar(p: float) -> str:
    k = int(max(0, min(1, p)) * 12)
    return "█" * k + "░" * (12 - k)


class Progress:
    """থ্রটলড প্রগ্রেস — প্রতি ~১০ সেকেন্ডে একবার এডিট (FloodWait এড়াতে)।"""

    def __init__(self, msg, label: str, pyro: bool = True):
        self.msg, self.label, self.pyro = msg, label, pyro
        self.t0 = time.time()
        self.last = 0.0

    async def __call__(self, cur, total):
        if cancel.is_set():
            if self.pyro:
                app.stop_transmission()
            raise Cancelled()
        now = time.time()
        if now - self.last < 10 and cur < total:
            return
        self.last = now
        sp = cur / max(now - self.t0, 1)
        eta = (total - cur) / sp if sp else 0
        txt = (f"{self.label}\n{bar(cur / total)} {cur / total * 100:.1f}%\n"
               f"{hsize(cur)} / {hsize(total)}\n⚡ {hsize(sp)}/s · ⏳ {hdur(eta)}")
        try:
            await self.msg.edit_text(txt)
        except FloodWait as e:
            self.last = now + e.value
        except Exception:
            pass


class TgSource(BlockSource):
    """টেলিগ্রাম ফাইলকে ডিস্কে না নামিয়ে ৪MB ব্লকে স্ট্রিম করে (RAM ≈ ৪-৮MB)।"""

    def __init__(self, msg, size: int):
        self.msg, self.size = msg, size

    async def get_block(self, i: int) -> bytes:
        per = BLOCK // (1024 * 1024)  # stream_media-র চাঙ্ক = ১MiB
        want = min(BLOCK, self.size - i * BLOCK)
        err = None
        for attempt in range(5):
            if cancel.is_set():
                raise Cancelled()
            try:
                parts = []
                async for c in app.stream_media(self.msg, limit=per, offset=i * per):
                    parts.append(c)
                data = b"".join(parts)
                if len(data) == want:
                    return data
                err = IOError(f"short block {len(data)}/{want}")
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                continue
            except Exception as e:
                err = e
            await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"টেলিগ্রাম থেকে ব্লক {i} পড়া যায়নি: {err}")


async def probe(path: Path) -> dict:
    p = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height:format=duration", "-of", "json", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    out, _ = await p.communicate()
    try:
        j = json.loads(out)
        st = j["streams"][0]
        return {"w": int(st["width"]), "h": int(st["height"]), "d": float(j["format"]["duration"])}
    except Exception:
        return {"w": 0, "h": 0, "d": 0.0}


async def make_thumb(path: Path, dur: float) -> Optional[str]:
    t = path.with_suffix(".jpg")
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-ss", str(min(5, max(0, dur / 2))), "-i", str(path),
        "-frames:v", "1", "-vf", "scale=320:-2", str(t),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    return str(t) if t.exists() else None


async def run_ffmpeg(src: Path, dst: Path, duration: float, status):
    global proc
    vf = f"scale=-2:'min({MAX_HEIGHT},ih)'"
    cmd = ["nice", "-n", "10", "ffmpeg", "-hide_banner", "-nostdin", "-y", "-i", str(src),
           "-map", "0:v:0", "-map", "0:a:0?", "-vf", vf,
           "-c:v", "libx264", "-preset", X264_PRESET, "-crf", CRF, "-pix_fmt", "yuv420p",
           "-threads", FF_THREADS, "-x264-params", "rc-lookahead=10:ref=2",
           "-c:a", "aac", "-b:a", AUDIO_BR, "-ac", "2",
           "-movflags", "+faststart", "-progress", "pipe:1", "-nostats", str(dst)]
    logf = dst.with_suffix(".log")
    with open(logf, "wb") as lf:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=lf)
        t0, last = time.time(), 0.0
        async for raw in proc.stdout:
            if cancel.is_set():
                proc.kill()
                break
            line = raw.decode(errors="ignore").strip()
            if not line.startswith(("out_time_us=", "out_time_ms=")):
                continue
            try:
                pos = int(line.split("=", 1)[1]) / 1e6
            except ValueError:
                continue
            now = time.time()
            if duration and now - last > 15:
                last = now
                frac = min(pos / duration, 1.0)
                eta = (now - t0) / frac - (now - t0) if frac > 0.01 else 0
                try:
                    await status.edit_text(f"🗜 কমপ্রেস হচ্ছে\n{bar(frac)} {frac * 100:.1f}%\n"
                                           f"⏳ ~{hdur(eta)} বাকি")
                except Exception:
                    pass
        rc = await proc.wait()
    proc = None
    if cancel.is_set():
        raise Cancelled()
    if rc != 0:
        tail = logf.read_text(errors="ignore")[-600:]
        raise RuntimeError(f"ffmpeg ব্যর্থ (code {rc}):\n{tail}")


# ───────────────────────── কাজ প্রসেসিং ─────────────────────────
async def upload_to_terabox(src: BlockSource, name: str, status) -> dict:
    if not tb:
        raise RuntimeError("TERABOX_NDUS সেট করা নেই।")
    return await tb.upload(
        src, name,
        progress=Progress(status, "☁️ TeraBox আপলোড (পাস ২/২)", pyro=False),
        hash_progress=Progress(status, "🔍 ব্লক যাচাই (পাস ১/২)", pyro=False))


async def process(job: Job):
    msg = await app.get_messages(job.chat_id, job.msg_id)
    media = getattr(msg, "video", None) or getattr(msg, "document", None)
    if not media:
        raise RuntimeError("মিডিয়া পাওয়া যায়নি (মেসেজ ডিলিট হয়েছে?)")
    name = safe_name(media.file_name or f"video_{job.msg_id}.mp4")
    status = await app.send_message(job.chat_id, f"⏳ শুরু: {name}\nমোড: {MODES[job.mode]}",
                                    reply_to_message_id=job.msg_id)
    jobdir = WORKDIR / f"{job.chat_id}_{job.msg_id}"
    try:
        # ── মোড ১: ডিস্ক-ছাড়া স্ট্রিম আপলোড ──
        if job.mode == "upload":
            res = await upload_to_terabox(TgSource(msg, media.file_size), name, status)
            await done_msg(job, name, res)
            return

        # ── মোড ২/৩: ডিস্কে নামিয়ে কমপ্রেস ──
        free = shutil.disk_usage(WORKDIR).free
        need = int(media.file_size * 2.1)
        if free < need:
            raise RuntimeError(f"ডিস্কে জায়গা কম: ফ্রি {hsize(free)}, দরকার ~{hsize(need)}। "
                               "এই ফাইলের জন্য /mode দিয়ে 'upload' (ডিস্ক-ছাড়া) ব্যবহার করুন।")
        jobdir.mkdir(parents=True, exist_ok=True)
        src = jobdir / ("input" + (Path(name).suffix or ".mp4"))
        got = await app.download_media(msg, file_name=str(src),
                                       progress=Progress(status, "⬇️ ডাউনলোড"))
        if not got:
            raise Cancelled()

        info = await probe(src)
        out = jobdir / (Path(name).stem + "_compressed.mp4")
        await run_ffmpeg(src, out, info["d"], status)

        if out.stat().st_size >= src.stat().st_size * 0.95:  # কমপ্রেস লাভজনক হয়নি
            final, final_name, note = src, name, "ℹ️ কমপ্রেসে সাইজ কমেনি, অরিজিনাল রাখা হয়েছে।"
            out.unlink(missing_ok=True)
        else:
            final, final_name, note = out, out.name, ""
            src.unlink(missing_ok=True)  # ডিস্ক খালি করুন
        saved = f"{hsize(final.stat().st_size)}"

        if job.mode == "cupload":
            res = await upload_to_terabox(FileSource(str(final)), final_name, status)
            await done_msg(job, final_name, res, extra=f"{note}\nসাইজ: {saved}")
        else:  # compress → টেলিগ্রাম
            if final.stat().st_size > TG_LIMIT:
                raise RuntimeError(f"কমপ্রেসের পরেও {saved} — টেলিগ্রাম সীমা ২GB ছাড়িয়েছে। "
                                   "CRF বাড়ান বা MAX_HEIGHT কমান।")
            meta = await probe(final)
            thumb = await make_thumb(final, meta["d"])
            await app.send_video(job.chat_id, str(final), caption=f"{final_name}\n{saved} {note}".strip(),
                                 duration=int(meta["d"]), width=meta["w"], height=meta["h"],
                                 thumb=thumb, supports_streaming=True,
                                 reply_to_message_id=job.msg_id,
                                 progress=Progress(status, "⬆️ টেলিগ্রামে আপলোড"))
    finally:
        shutil.rmtree(jobdir, ignore_errors=True)  # সবসময় টেম্প ফাইল মুছুন
        try:
            await status.delete()
        except Exception:
            pass


async def done_msg(job: Job, name: str, res: dict, extra: str = ""):
    link = res.get("link") or "(শেয়ার লিংক তৈরি হয়নি — TeraBox অ্যাপে ফাইল দেখুন)"
    await app.send_message(job.chat_id, f"✅ TeraBox আপলোড সম্পন্ন\n📁 {res.get('path')}\n"
                                        f"🔗 {link}\n{extra}".strip(), reply_to_message_id=job.msg_id)


async def worker():
    global current, proc
    while True:
        job = await queue.get()
        if job not in pending:  # /clear দিয়ে মুছে ফেলা
            queue.task_done()
            continue
        pending.remove(job)
        current = job
        cancel.clear()
        save_state()
        try:
            await process(job)
        except Cancelled:
            await safe_send(job.chat_id, f"🛑 বাতিল: {job.name}")
        except TeraBoxError as e:
            await safe_send(job.chat_id, f"❌ TeraBox ত্রুটি: {job.name}\n{e}\n"
                                         "কুকি এক্সপায়ার হলে নতুন ndus দিন (/tbtest)।")
        except Exception as e:
            log.exception("job failed")
            await safe_send(job.chat_id, f"❌ ব্যর্থ: {job.name}\n{str(e)[:700]}")
        finally:
            current, proc = None, None
            save_state()
            queue.task_done()


async def safe_send(chat_id, text):
    try:
        await app.send_message(chat_id, text)
    except Exception:
        log.exception("send failed")


# ───────────────────────── হ্যান্ডলার ─────────────────────────
owner = filters.user(list(OWNER_IDS))
_ack: dict[int, asyncio.Task] = {}


@app.on_message(filters.command(["start", "help"]) & owner & filters.private)
async def cmd_start(_, m):
    await m.reply(
        "ভিডিও ফরওয়ার্ড করুন — কিউতে যোগ হবে, একটার পর একটা প্রসেস হবে।\n\n"
        "/mode — মোড বদলান\n/queue — কিউ দেখুন\n/cancel — চলমান কাজ বাতিল\n"
        "/clear — অপেক্ষমান সব মুছুন\n/disk — ডিস্ক ফ্রি\n/tbtest — TeraBox কুকি টেস্ট\n\n"
        f"বর্তমান মোড: {MODES[modes.get(m.from_user.id, DEFAULT_MODE)]}")


@app.on_message(filters.command("mode") & owner & filters.private)
async def cmd_mode(_, m):
    cur = modes.get(m.from_user.id, DEFAULT_MODE)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(("✅ " if k == cur else "") + v, callback_data=f"mode:{k}")]
                               for k, v in MODES.items()])
    await m.reply("প্রসেসিং মোড বাছুন:", reply_markup=kb)


@app.on_callback_query(filters.regex(r"^mode:") & owner)
async def cb_mode(_, q):
    k = q.data.split(":", 1)[1]
    if k in MODES:
        modes[q.from_user.id] = k
        save_state()
        await q.answer("সেট হয়েছে")
        await q.message.edit_text(f"✅ মোড: {MODES[k]}")


@app.on_message(filters.command("queue") & owner & filters.private)
async def cmd_queue(_, m):
    lines = [f"▶️ চলছে: {current.name}" if current else "▶️ কিছু চলছে না"]
    lines += [f"{i}. {j.name} ({hsize(j.size)})" for i, j in enumerate(pending[:20], 1)]
    if len(pending) > 20:
        lines.append(f"… আরও {len(pending) - 20}টি")
    await m.reply("\n".join(lines))


@app.on_message(filters.command("cancel") & owner & filters.private)
async def cmd_cancel(_, m):
    if not current:
        return await m.reply("চলমান কিছু নেই।")
    cancel.set()
    if proc and proc.returncode is None:
        proc.kill()
    await m.reply("🛑 বাতিল করা হচ্ছে…")


@app.on_message(filters.command("clear") & owner & filters.private)
async def cmd_clear(_, m):
    n = len(pending)
    pending.clear()
    save_state()
    await m.reply(f"🧹 {n}টি অপেক্ষমান কাজ মুছেছে।")


@app.on_message(filters.command("disk") & owner & filters.private)
async def cmd_disk(_, m):
    d = shutil.disk_usage(WORKDIR)
    await m.reply(f"💾 মোট {hsize(d.total)} · ব্যবহৃত {hsize(d.used)} · ফ্রি {hsize(d.free)}")


@app.on_message(filters.command("tbtest") & owner & filters.private)
async def cmd_tbtest(_, m):
    if not tb:
        return await m.reply("TERABOX_NDUS সেট করা নেই।")
    try:
        js = await tb.check()
        await m.reply(f"✅ কুকি ঠিক আছে\nমোট: {hsize(js.get('total', 0))} · ব্যবহৃত: {hsize(js.get('used', 0))}")
    except Exception as e:
        await m.reply(f"❌ {e}")


@app.on_message((filters.video | filters.document) & owner & filters.private)
async def on_media(_, m):
    media = m.video or m.document
    if m.document and not (media.mime_type or "").startswith("video/"):
        return
    if len(pending) >= MAX_QUEUE:
        return await m.reply(f"কিউ ভর্তি (সর্বোচ্চ {MAX_QUEUE})।")
    mode = modes.get(m.from_user.id, DEFAULT_MODE)
    job = Job(m.chat.id, m.id, mode, safe_name(media.file_name or f"video_{m.id}.mp4"), media.file_size)
    pending.append(job)
    save_state()
    await queue.put(job)

    # একসাথে অনেক ফরওয়ার্ড করলে ১টি সারাংশ মেসেজ (স্প্যাম/FloodWait এড়াতে)
    t = _ack.get(m.chat.id)
    if t and not t.done():
        t.cancel()

    async def later():
        await asyncio.sleep(3)
        await safe_send(m.chat.id, f"✅ কিউতে যোগ হয়েছে। অপেক্ষমান: {len(pending)} · মোড: {MODES[mode]}")

    _ack[m.chat.id] = asyncio.create_task(later())


# ───────────────────────── ওয়েব সার্ভার + কিপ-অ্যালাইভ ─────────────────────────
async def start_web():
    async def root(_):
        return web.Response(text="ok")

    async def stat(_):
        return web.json_response({"pending": len(pending), "current": current.name if current else None})

    wa = web.Application()
    wa.add_routes([web.get("/", root), web.get("/status", stat)])
    runner = web.AppRunner(wa)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def keepalive():
    """Render ফ্রি ১৫ মিনিট ইনবাউন্ড ট্রাফিক না পেলে ঘুমায়; নিজের পাবলিক URL-এ পিং করলে জেগে থাকে।"""
    url = os.getenv("KEEPALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    async with aiohttp.ClientSession() as s:
        while True:
            await asyncio.sleep(600)
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    await r.read()
            except Exception:
                pass


async def main():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    for d in WORKDIR.iterdir():  # আগের রান-এর অবশিষ্ট টেম্প ফাইল মুছুন
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
    restored = load_state()
    await start_web()
    await app.start()
    for j in restored:
        pending.append(j)
        await queue.put(j)
    asyncio.create_task(worker())
    asyncio.create_task(keepalive())
    for uid in OWNER_IDS:
        await safe_send(uid, f"🟢 বট চালু। পুনরুদ্ধার করা কাজ: {len(restored)}টি\n"
                             "(সার্ভার রিস্টার্ট হলে ডিস্ক মুছে যায়, তাই কিউ হারিয়ে যেতে পারে — /queue দেখুন)")
    await idle()
    await app.stop()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
