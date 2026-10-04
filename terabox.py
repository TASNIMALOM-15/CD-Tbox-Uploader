"""
TeraBox আনঅফিশিয়াল ওয়েব-API আপলোডার (কুকি-ভিত্তিক)।

⚠️ TeraBox-এর কোনো অফিশিয়াল পাবলিক আপলোড API নেই। এই কোড ওয়েব-অ্যাপের এন্ডপয়েন্ট
(precreate -> superfile2 -> create) অনুসরণ করে। TeraBox এগুলো বদলালে ভেঙে যেতে পারে।
প্রথমে /tbtest দিয়ে কুকি চেক করুন, তারপর ছোট ফাইল দিয়ে টেস্ট করুন।

ডিজাইন: ফাইলকে ৪MB ব্লকে ভাগ করে ব্লক-বাই-ব্লক পড়া হয় (BlockSource), তাই RAM ব্যবহার ~১০-২০MB।
BlockSource ডিস্ক-ফাইল হতে পারে, আবার সরাসরি টেলিগ্রাম স্ট্রিমও হতে পারে (ডিস্ক ছাড়াই)।
"""
import asyncio
import hashlib
import json
import os
import random
import re
import time

import aiohttp

BLOCK = 4 * 1024 * 1024
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
COMMON = {"app_id": "250528", "web": "1", "channel": "dubox", "clienttype": "0"}


class TeraBoxError(Exception):
    pass


class BlockSource:
    size: int = 0

    @property
    def n_blocks(self) -> int:
        return max(1, -(-self.size // BLOCK))

    async def get_block(self, i: int) -> bytes:  # pragma: no cover
        raise NotImplementedError


class FileSource(BlockSource):
    def __init__(self, path: str):
        self.path = path
        self.size = os.path.getsize(path)

    async def get_block(self, i: int) -> bytes:
        def _read():
            with open(self.path, "rb") as f:
                f.seek(i * BLOCK)
                return f.read(BLOCK)
        return await asyncio.to_thread(_read)


class TeraBox:
    def __init__(self, cookie: str, remote_dir: str = "/TGBot"):
        cookie = cookie.strip()
        # শুধু ndus এর মান দিলেও চলবে, পুরো কুকি স্ট্রিং দিলেও চলবে
        self.cookie = cookie if "=" in cookie else f"ndus={cookie}"
        self.remote_dir = "/" + remote_dir.strip("/")
        self.base = "https://www.terabox.com"
        self.js_token = ""

    # ---------- helpers ----------
    def _h(self) -> dict:
        return {
            "User-Agent": UA,
            "Cookie": self.cookie,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"{self.base}/main",
            "Origin": self.base,
        }

    def _p(self, **kw) -> dict:
        p = dict(COMMON)
        p["jsToken"] = self.js_token
        p["dp-logid"] = f"{int(time.time() * 1000)}{random.randint(100000, 999999)}"
        p.update({k: str(v) for k, v in kw.items()})
        return p

    @staticmethod
    def _session() -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300))

    async def _bootstrap(self, s: aiohttp.ClientSession):
        async with s.get(f"{self.base}/main?category=all", headers=self._h()) as r:
            html = await r.text()
            self.base = f"{r.url.scheme}://{r.url.host}"  # রিডাইরেক্ট হলে ডোমেইন আপডেট
        m = re.search(r"fn%28%22(.+?)%22%29", html)
        if not m:
            raise TeraBoxError("jsToken পাওয়া যায়নি — কুকি (ndus) মেয়াদোত্তীর্ণ/ভুল, "
                               "অথবা TeraBox পেজ কাঠামো বদলেছে।")
        self.js_token = m.group(1)

    async def _api(self, s, method: str, path: str, params: dict, data: dict | None = None) -> dict:
        async with s.request(method, f"{self.base}{path}", params=params,
                             data=data, headers=self._h()) as r:
            txt = await r.text()
        try:
            return json.loads(txt)
        except ValueError:
            raise TeraBoxError(f"{path} থেকে JSON আসেনি: {txt[:200]}")

    # ---------- public ----------
    async def check(self) -> dict:
        """কুকি ঠিক আছে কি না যাচাই + স্টোরেজ কোটা রিটার্ন।"""
        async with self._session() as s:
            await self._bootstrap(s)
            js = await self._api(s, "GET", "/api/quota", self._p(checkfree=1, checkexpire=1))
        if js.get("errno") != 0:
            raise TeraBoxError(f"quota errno={js.get('errno')}: {js}")
        return js

    async def upload(self, src: BlockSource, filename: str, progress=None, hash_progress=None) -> dict:
        remote = f"{self.remote_dir}/{filename}"
        async with self._session() as s:
            await self._bootstrap(s)

            # পাস ১: প্রতিটি ব্লকের MD5 (precreate-এর জন্য আগে লাগে)
            md5s = []
            for i in range(src.n_blocks):
                md5s.append(hashlib.md5(await src.get_block(i)).hexdigest())
                if hash_progress:
                    await hash_progress(min((i + 1) * BLOCK, src.size), src.size)

            now = int(time.time())
            pre = await self._api(s, "POST", "/api/precreate", self._p(), {
                "path": remote, "autoinit": 1, "target_path": self.remote_dir + "/",
                "block_list": json.dumps(md5s), "size": src.size, "rtype": 1,
                "local_ctime": now, "local_mtime": now, "file_limit_switch_v34": "true"})
            if pre.get("errno") != 0 or not pre.get("uploadid"):
                raise TeraBoxError(f"precreate ব্যর্থ: {pre}")
            uploadid = pre["uploadid"]
            host = await self._locate(s, remote, uploadid)

            # পাস ২: ব্লক আপলোড
            for i in range(src.n_blocks):
                data = await src.get_block(i)
                await self._put_block(s, host, remote, uploadid, i, data, md5s[i])
                if progress:
                    await progress(min((i + 1) * BLOCK, src.size), src.size)

            res = await self._api(s, "POST", "/api/create", self._p(), {
                "path": remote, "size": src.size, "uploadid": uploadid,
                "target_path": self.remote_dir + "/", "block_list": json.dumps(md5s),
                "isdir": 0, "rtype": 1, "local_mtime": now})
            if res.get("errno") != 0:
                raise TeraBoxError(f"create ব্যর্থ: {res}")

            link = None
            try:
                link = await self._share(s, res.get("fs_id"))
            except Exception:
                pass  # শেয়ার লিংক ঐচ্ছিক; ফাইল আপলোড হয়ে গেছে
            return {"path": res.get("path", remote), "fs_id": res.get("fs_id"), "link": link}

    # ---------- internals ----------
    async def _locate(self, s, remote: str, uploadid: str) -> str:
        try:
            p = {"method": "locateupload", "appid": COMMON["app_id"], "path": remote,
                 "uploadid": uploadid, "upload_version": "2.0"}
            async with s.get("https://d.terabox.com/rest/2.0/pcs/file", params=p,
                             headers=self._h()) as r:
                js = await r.json(content_type=None)
            host = (js.get("host") or "").replace("https://", "").strip("/")
            if host:
                return host
        except Exception:
            pass
        return "c-jp.terabox.com"

    async def _put_block(self, s, host, remote, uploadid, i, data: bytes, md5: str):
        params = self._p(method="upload", path=remote, uploadid=uploadid, uploadsign=0, partseq=i)
        url = f"https://{host}/rest/2.0/pcs/superfile2"
        err = None
        for attempt in range(5):
            try:
                fd = aiohttp.FormData()
                fd.add_field("file", data, filename="blob", content_type="application/octet-stream")
                async with s.post(url, params=params, data=fd, headers=self._h()) as r:
                    js = await r.json(content_type=None)
                if js.get("md5") == md5:
                    return
                if "md5" not in js and not js.get("error_code") and not js.get("errno"):
                    return
                err = js
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                err = e
            await asyncio.sleep(min(2 ** attempt, 30))
        raise TeraBoxError(f"ব্লক {i} আপলোড ব্যর্থ: {err}")

    async def _share(self, s, fs_id):
        if not fs_id:
            return None
        js = await self._api(s, "POST", "/share/set", self._p(), {
            "period": 0, "pwd": "", "eflag_disable": "true", "channel_list": "[]",
            "schannel": 4, "fid_list": json.dumps([int(fs_id)])})
        return js.get("link") if js.get("errno") == 0 else None
