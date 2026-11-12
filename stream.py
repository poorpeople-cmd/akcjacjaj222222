import asyncio
import os
import sys
import subprocess
from camoufox.async_api import AsyncCamoufox

# ==============================================================================
# Paste your full STREAM_KEYS dictionary below (Shortened here for brevity)
STREAM_KEYS = {
    "1": "15254238731883_15281627925099_najspfkgne",
    "1.1": "15254260751979_15281671637611_2plrcfqzze",
    "s1.1": "14204232736303_14846150314543_37jq4ryehq"
}
# ==============================================================================

FB_INGEST = "rtmps://live-api-s.facebook.com:443/rtmp/"
SOURCE_URL = os.environ.get("SOURCE_URL")
KEY_INPUT = os.environ.get("STREAM_KEY", "1").strip()
QUALITY = os.environ.get("STREAM_QUALITY", "Original (1080p Max)")
DURATION = int(os.environ.get("DURATION_MINUTES", "180"))

if not SOURCE_URL:
    print("[!] ERROR: SOURCE_URL is required.")
    sys.exit(1)

# Resolve RTMP URL
if KEY_INPUT in STREAM_KEYS:
    RTMP_URL = f"{FB_INGEST}{STREAM_KEYS[KEY_INPUT]}"
elif KEY_INPUT.startswith("rtmp"):
    RTMP_URL = KEY_INPUT
else:
    RTMP_URL = f"{FB_INGEST}{KEY_INPUT}"

# Resolve Video Quality
RES_W, RES_H, BITRATE = 1920, 1080, 5000
if QUALITY == "360p": RES_W, RES_H, BITRATE = 640, 360, 800
elif QUALITY == "480p": RES_W, RES_H, BITRATE = 854, 480, 1500
elif QUALITY == "720p": RES_W, RES_H, BITRATE = 1280, 720, 3000
elif QUALITY == "1080p": RES_W, RES_H, BITRATE = 1920, 1080, 4500

ffmpeg_process = None

def start_ffmpeg(m3u8_url, headers):
    global ffmpeg_process
    if ffmpeg_process is not None:
        return

    print(f"[📡] Captured M3U8 URL: {m3u8_url}")
    
    # Format headers for FFmpeg
    header_list = [f"{k}: {v}" for k, v in headers.items()]
    header_str = "\r\n".join(header_list) + "\r\n"

    encoding_args = ["-c", "copy", "-bsf:a", "aac_adtstoasc"]
    if QUALITY != "Original (1080p Max)":
        encoding_args = [
            "-vf", f"scale={RES_W}:{RES_H}",
            "-c:v", "libx264", "-preset", "veryfast",
            "-b:v", f"{BITRATE}k", "-maxrate", f"{BITRATE}k",
            "-bufsize", f"{BITRATE*2}k", "-g", "60",
            "-c:a", "aac", "-b:a", "128k"
        ]

    args = [
        "ffmpeg", "-hide_banner", "-loglevel", "info",
        "-headers", header_str,
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-i", m3u8_url,
        *encoding_args,
        "-f", "flv", RTMP_URL
    ]
    
    ffmpeg_process = subprocess.Popen(args)

async def main():
    print(f"[🚀] Starting Camoufox Engine: {QUALITY} @ {BITRATE}kbps")
    
    # Launch Camoufox (headless=False is required for xvfb compatibility)
    async with AsyncCamoufox(headless=False, geoip=True) as browser:
        page = await browser.new_page()
        
        # Network interceptor for .m3u8 files
        async def handle_request(request):
            if ffmpeg_process is not None:
                return
            if ".m3u8" in request.url:
                headers = await request.all_headers()
                # Pass only essential secure headers to FFmpeg
                filtered = {k: v for k, v in headers.items() if k.lower() in ['referer', 'user-agent', 'cookie', 'origin']}
                start_ffmpeg(request.url, filtered)

        page.on("request", handle_request)
        
        try:
            await page.goto(SOURCE_URL, timeout=60000)
        except Exception as e:
            print(f"[!] Page loaded with warnings: {e}")

        # Wait up to 90 seconds for a stream token to be detected
        timeout = 90
        while ffmpeg_process is None and timeout > 0:
            await asyncio.sleep(1)
            timeout -= 1
            
        if ffmpeg_process is None:
            print("[🛑] Timeout: No .m3u8 stream detected within 90 seconds.")
            sys.exit(1)
            
        print("[🧹] Stream stable. Camoufox browser automatically closing to free runner RAM...")

    # At this indentation level, `async with` has closed the browser.
    # We now wait for FFmpeg to finish broadcasting.
    try:
        ffmpeg_process.wait(timeout=DURATION * 60)
    except subprocess.TimeoutExpired:
        print(f"[🛑] Reached {DURATION} minute limit. Stopping stream...")
        ffmpeg_process.terminate()

if __name__ == "__main__":
    asyncio.run(main())
