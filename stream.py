import asyncio
import os
import sys
import subprocess
from urllib.parse import urlparse
from camoufox.async_api import AsyncCamoufox

# ==============================================================================
# 🚀 Multi-Stream Key Manager
STREAM_KEYS = {
    "1": "15254238731883_15281627925099_najspfkgne",
    "1.1": "15254260751979_15281671637611_2plrcfqzze",
    "1.2": "15254285524587_15281717840491_7e6qdknzsu",

    "1.3": "16983200374379_18360218880619_efmw4fchxm",
    "1.4": "16983242579563_18360288741995_t2kh2jvcym",
    "1.5": "16983247822443_18360297982571_3zs5ml7gua",
    "1.6": "16983254834795_18360310237803_xrz3pf4uea",
    "1.7": "16983259553387_18360317971051_6khoe5efge",
    "1.8": "16983264468587_18360327342699_wsn7zgr7ee",
    "1.9": "16983269514859_18360335075947_363qzjlbte",
    "1.10": "16983277641323_18360347921003_rpf6hkqpdm",
    "1.11": "16983286554219_18360360503915_lrqraxbz3y",

    "2": "15254299352683_15281743071851_7dvz3h5d7q",
    "2.1": "15254308986475_15281761618539_3xca7oij3u",
    "2.2": "15254328122987_15281795566187_zjqa6bqzoq",

    "3": "15254341885547_15281821059691_hhlpb5vicy",
    "3.1": "15254357089899_15281848322667_sxeexgvzl4",
    "3.2": "15254367510123_15281868180075_pc4jrytfgm",
    "3.3": "17485986405995_19233134873195_iw63xjblny",
    "3.4": "17485996039787_19233150929515_d3yeoweuam",
    "3.5": "17486002789995_19233161611883_mbg22slybe",
    "3.6": "17486007770731_19233169934955_teme2zt4fm",

    "4": "15255022345835_15283095800427_vwrupxzstm",
    "4.1": "15255038074475_15283122080363_ai5qqp2we4",
    "4.2": "15255045480043_15283135842923_tldl4bhmii",
    "4.3": "15255208599147_15283449629291_abltofuc7m",
    "4.4": "15255217708651_15283466603115_bojrrqtlmu",
    "4.5": "15255227670123_15283486263915_jpntt54mve",

    # "5": "15273689226859_15317451606635_d7zzy3c7qi",
    "5": "17135767461483_18625099401835_4gpytlxk4a",
    "5.1": "15273713933931_15317494860395_avj47smmim",
    "5.2": "15273722257003_15317510195819_6edjluvdqi",
    "5.3": "15273739624043_15317541653099_ii4bxpvabe",
    "5.4": "15273750175339_15317561707115_csel26ku5a",
    "5.5": "15273760071275_15317579467371_cnewcj54me",
    "5.6": "15273767935595_15317595851371_3q43tk7tvm",
    "5.7": "15273778683499_15317616560747_4piekvs4wu",

    "s1.1": "14204232736303_14846150314543_37jq4ryehq",
    "s1.2": "14204288179759_14846247373359_tnsknmapva",
    "s1.3": "14204319768111_14846302489135_sr4ht4ccwq",
    "s1.4": "14204331957807_14846326147631_dji2acqcze",
    "s1.5": "14204346572335_14846351641135_7gvns4o5ue",
    "s1.6": "14204361252399_14846376479279_cjajhf4d3y",
    "s1.7": "14204370492975_14846393649711_6fduhdqite",
    "s1.8": "14204395527727_14846438017583_s2jlti7lsm",
    "s1.9": "14204411387439_14846464887343_f5lxgcqj5y",
    "s1.10": "14204424691247_14846487562799_xmbvntt6wa",

    "s2.1": "14204490948143_14846603495983_kzevn36tii",
    "s2.2": "14204506742319_14846634494511_ta2rxyg2oy",
    "s2.3": "14204523322927_14846661233199_foqb3q7zb4",
    "s2.4": "14204540034607_14846689085999_gjejdie4uy",
    "s2.5": "14204555304495_14846715497007_zdanghuxzu",
    "s2.6": "14204565200431_14846734371375_ap3bqpabpu",
    "s2.7": "14204577259055_14846756194863_3ecad2535u",
    "s2.8": "14204592528943_14846785227311_4hjl46y62e",
    "s2.9": "14204602621487_14846802594351_ilnp6lxekq",
    "s2.10": "14206184136239_14849618610735_ihnbx7hkoi"
}
# ==============================================================================

FB_INGEST = "rtmps://live-api-s.facebook.com:443/rtmp/"
SOURCE_URL = os.environ.get("SOURCE_URL")
KEY_INPUT = os.environ.get("STREAM_KEY", "1").strip()
QUALITY = os.environ.get("STREAM_QUALITY", "Original (1080p Max)")
DURATION = int(os.environ.get("DURATION_MINUTES", "180"))
PROXY_URL = os.environ.get("PROXY_URL", "").strip()
PROXY_FFMPEG = os.environ.get("PROXY_FFMPEG", "true").strip().lower() == "true"

if not SOURCE_URL:
    print("[!] ERROR: SOURCE_URL is required.")
    sys.exit(1)

# Parse PROXY_URL into Playwright/Camoufox proxy dictionary
def parse_proxy(raw_url):
    if not raw_url:
        return None
    if "://" not in raw_url:
        raw_url = f"http://{raw_url}"
    parsed = urlparse(raw_url)
    if not parsed.hostname or not parsed.port:
        print("[!] ERROR: Invalid PROXY_URL format. Expected http://user:pass@host:port")
        sys.exit(1)
    config = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    return config

proxy_config = parse_proxy(PROXY_URL)
normalized_proxy_url = (
    f"http://{PROXY_URL}" if PROXY_URL and "://" not in PROXY_URL else PROXY_URL
)

# Resolve RTMP URL
if KEY_INPUT in STREAM_KEYS:
    RTMP_URL = f"{FB_INGEST}{STREAM_KEYS[KEY_INPUT]}"
elif KEY_INPUT.startswith("rtmp"):
    RTMP_URL = KEY_INPUT
else:
    RTMP_URL = f"{FB_INGEST}{KEY_INPUT}"

# Resolve Video Quality
RES_W, RES_H, BITRATE = 1920, 1080, 5000
if QUALITY == "360p":
    RES_W, RES_H, BITRATE = 640, 360, 800
elif QUALITY == "480p":
    RES_W, RES_H, BITRATE = 854, 480, 1500
elif QUALITY == "720p":
    RES_W, RES_H, BITRATE = 1280, 720, 3000
elif QUALITY == "1080p":
    RES_W, RES_H, BITRATE = 1920, 1080, 4500

ffmpeg_process = None

def start_ffmpeg(m3u8_url, headers):
    global ffmpeg_process
    if ffmpeg_process is not None:
        return

    print(f"[📡] Captured M3U8 URL: {m3u8_url}")

    header_list = [f"{k}: {v}" for k, v in headers.items()]
    header_str = "\r\n".join(header_list) + "\r\n"

    proxy_args = []
    if normalized_proxy_url and PROXY_FFMPEG:
        print("[🛡️] Routing FFmpeg HLS pull through HTTP proxy")
        proxy_args = ["-http_proxy", normalized_proxy_url]

    encoding_args = ["-c", "copy", "-bsf:a", "aac_adtstoasc"]
    if QUALITY != "Original (1080p Max)":
        encoding_args = [
            "-vf", f"scale={RES_W}:{RES_H}",
            "-c:v", "libx264", "-preset", "veryfast",
            "-b:v", f"{BITRATE}k", "-maxrate", f"{BITRATE}k",
            "-bufsize", f"{BITRATE * 2}k", "-g", "60",
            "-c:a", "aac", "-b:a", "128k"
        ]

    args = [
        "ffmpeg", "-hide_banner", "-loglevel", "info",
        *proxy_args,
        "-headers", header_str,
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-i", m3u8_url,
        *encoding_args,
        "-f", "flv", RTMP_URL
    ]

    ffmpeg_process = subprocess.Popen(args)

async def main():
    if proxy_config:
        print(f"[🛡️] Residential Proxy Active: {proxy_config['server']} (GeoIP Auto-Match ON)")
    else:
        print("[🌐] Direct Connection Mode (No Proxy Configured)")

    print(f"[🚀] Starting Camoufox Engine: {QUALITY} @ {BITRATE}kbps")

    async with AsyncCamoufox(
        headless=False,
        proxy=proxy_config,
        geoip=bool(proxy_config)
    ) as browser:
        page = await browser.new_page()

        async def handle_request(request):
            if ffmpeg_process is not None:
                return
            if ".m3u8" in request.url:
                headers = await request.all_headers()
                filtered = {
                    k: v for k, v in headers.items()
                    if k.lower() in ["referer", "user-agent", "cookie", "origin"]
                }
                start_ffmpeg(request.url, filtered)

        page.on("request", handle_request)

        try:
            await page.goto(SOURCE_URL, timeout=60000)
        except Exception as e:
            print(f"[!] Page loaded with warnings: {e}")

        timeout = 90
        while ffmpeg_process is None and timeout > 0:
            await asyncio.sleep(1)
            timeout -= 1

        if ffmpeg_process is None:
            print("[🛑] Timeout: No .m3u8 stream detected within 90 seconds.")
            sys.exit(1)

        print("[🧹] Stream stable. Camoufox browser closing to free runner RAM...")

    try:
        ffmpeg_process.wait(timeout=DURATION * 60)
    except subprocess.TimeoutExpired:
        print(f"[🛑] Reached {DURATION} minute limit. Stopping stream...")
        ffmpeg_process.terminate()

if __name__ == "__main__":
    asyncio.run(main())
