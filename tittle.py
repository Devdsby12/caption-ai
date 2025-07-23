import os
import json
import random
import asyncio
import aiohttp
import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import subprocess
import logging
from datetime import datetime
import gc
import signal

# --- Configure Logging ---
LOG_FILE = os.path.expanduser("~/ui/automation.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),  # Append mode to prevent overwrites
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Signal Handling ---
def handle_signal(signum, frame):
    logger.info(f"Received signal {signum}, performing graceful shutdown...")
    send_telegram("🛑 Received termination signal, shutting down gracefully...")
    raise KeyboardInterrupt()

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# --- Load Environment Variables ---
load_dotenv(dotenv_path=os.path.expanduser("~/ui/.env"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Gemini Configuration
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
GEMINI_HEADERS = {"Content-Type": "application/json", "X-goog-api-key": GEMINI_API_KEY} if GEMINI_API_KEY else None

# --- Path Configuration ---
BASE_DIR = os.path.expanduser("~/ui")
COMBO_DIR = os.path.join(BASE_DIR, "combo")
os.makedirs(COMBO_DIR, exist_ok=True)
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.json")
HEALTHCHECK_FILE = os.path.join(BASE_DIR, "healthcheck")

# Instagram Configuration
REELS_URL = "https://www.instagram.com/reels/"
CAPTION_SPAN_SELECTOR = 'span[class*="x193iq5w"][style*="line-height"]'

# Timing Configuration (in seconds)
MIN_CYCLE_DELAY = 3
MAX_CYCLE_DELAY = 16

# Resource Management
MAX_MEMORY_RETRIES = 3
MEMORY_RETRY_DELAY = 60

# --- Enhanced Health Check System ---
def update_healthcheck():
    try:
        with open(HEALTHCHECK_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        logger.error(f"Failed to update healthcheck: {e}")

# --- Enhanced Error Handling Utilities ---
async def execute_with_retries(account_name, operation, max_retries=3, delay=5, operation_name="operation"):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = await operation()
            if attempt > 1:
                logger.info(f"[{account_name}] {operation_name} succeeded on attempt {attempt}")
            return result
        except Exception as e:
            last_error = e
            logger.warning(f"[{account_name}] Attempt {attempt}/{max_retries} failed for {operation_name}: {str(e)}")
            if attempt < max_retries:
                await asyncio.sleep(delay * attempt)
    raise last_error if last_error else Exception(f"All {max_retries} attempts failed for {operation_name}")

async def safe_goto(page, url, account_name, timeout=60000):
    try:
        await asyncio.wait_for(page.goto(url, timeout=timeout), timeout=timeout/1000 + 10)
        return True
    except PlaywrightTimeoutError:
        logger.error(f"[{account_name}] Timeout loading page: {url}")
        send_telegram(f"⌛ Timeout loading page for {account_name}: {url}", account_name)
    except Exception as e:
        logger.error(f"[{account_name}] Error loading page {url}: {e}")
        send_telegram(f"❌ Error loading page for {account_name}: {e}", account_name)
    return False

async def safe_browser_launch(browser_type, account_name, **kwargs):
    try:
        # Add memory constraints for low-RAM environments
        kwargs.setdefault('args', []).extend([
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            '--single-process'
        ])
        browser = await browser_type.launch(**kwargs)
        return browser
    except Exception as e:
        logger.error(f"[{account_name}] Failed to launch browser: {e}")
        send_telegram(f"❌ Browser launch failed for {account_name}: {e}", account_name)
    return None

async def memory_safe_operation(account_name, operation, operation_name):
    for attempt in range(1, MAX_MEMORY_RETRIES + 1):
        try:
            result = await operation()
            return result
        except MemoryError:
            logger.warning(f"[{account_name}] Memory error during {operation_name}, attempt {attempt}/{MAX_MEMORY_RETRIES}")
            if attempt < MAX_MEMORY_RETRIES:
                await asyncio.sleep(MEMORY_RETRY_DELAY * attempt)
                gc.collect()
            else:
                raise
        except Exception as e:
            raise e

# --- Original Utility Functions (unchanged core functionality) ---
def send_telegram(message, account_name=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured")
        return
    if account_name:
        message = f"[{account_name}] {message}"
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        response = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def save_screenshot(page, step_name, account_name=None):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"debug_{account_name}_{step_name}_{timestamp}.png" if account_name else f"debug_{step_name}_{timestamp}.png"
    path = os.path.join(BASE_DIR, filename)
    try:
        if account_name:
            # Keep only the latest 3 screenshots per account
            existing = sorted([f for f in os.listdir(BASE_DIR) if f.startswith(f"debug_{account_name}_") and f.endswith(".png")])
            for f in existing[:-3]:  # Keep last 3
                try:
                    os.remove(os.path.join(BASE_DIR, f))
                except Exception:
                    pass
        await page.screenshot(path=path, full_page=True)
        return path
    except Exception as e:
        logger.error(f"Screenshot failed for {step_name}: {e}")
        return None

def clean_temp_files(account_name=None):
    temp_files = [
        os.path.join(BASE_DIR, f"reel_{account_name}.mp4") if account_name else os.path.join(BASE_DIR, "reel_temp.mp4"),
        os.path.join(BASE_DIR, f"reel_{account_name}_processed.mp4") if account_name else os.path.join(BASE_DIR, "reel_processed_temp.mp4"),
        os.path.join(BASE_DIR, f"caption_{account_name}.txt") if account_name else os.path.join(BASE_DIR, "caption_temp.txt"),
    ]
    for file_path in temp_files:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to remove temp file {file_path}: {e}")
    # Clean screenshots
    if account_name:
        # Keep only the latest 3 screenshots
        existing = sorted([f for f in os.listdir(BASE_DIR) if f.startswith(f"debug_{account_name}_") and f.endswith(".png")])
        for f in existing[:-3]:  # Keep last 3
            try:
                os.remove(os.path.join(BASE_DIR, f))
            except Exception as e:
                logger.warning(f"Failed to remove screenshot {f}: {e}")

def truncate_log_file():
    try:
        # Instead of truncating, rotate logs to keep last 100KB
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 100 * 1024:  # 100KB
            with open(LOG_FILE, 'r') as f:
                lines = f.readlines()
            with open(LOG_FILE, 'w') as f:
                f.writelines(lines[-1000:])  # Keep last 1000 lines
    except Exception as e:
        logger.error(f"Failed to rotate log file: {e}")

def load_account_config(account_dir):
    config_path = os.path.join(account_dir, "custom.json")
    cookie_path = os.path.join(account_dir, "cookies.json")
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"custom.json not found in {account_dir}")
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        if not isinstance(config, dict):
            raise ValueError("custom.json should contain a JSON object, not an array")
        if not os.path.exists(cookie_path):
            raise FileNotFoundError(f"cookies.json not found in {account_dir}")
        with open(cookie_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        if not isinstance(cookies, list):
            raise ValueError("cookies.json should contain a JSON array of cookie objects")
        return {
            "username": config.get("username", "@unknown"),
            "hashtags": config.get("hashtags", []),
            "custom_caption": config.get("custom_caption", ""),
            "use_custom_caption": config.get("use_custom_caption", False),
            "cookies": cookies,
            "account_name": os.path.basename(account_dir)
        }
    except Exception as e:
        logger.error(f"Failed to load config for {account_dir}: {e}")
        send_telegram(f"❌ Config load failed for {os.path.basename(account_dir)}: {e}")
        return None

def save_last_run(account_name):
    try:
        with open(LAST_RUN_FILE, "w") as f:
            json.dump({"last_account": account_name, "timestamp": datetime.now().isoformat()}, f)
    except Exception as e:
        logger.error(f"Failed to save last run info: {e}")

def get_next_account(account_dirs):
    try:
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, "r") as f:
                last_run = json.load(f)
            last_account = last_run.get("last_account")
            if last_account:
                last_index = next((i for i, d in enumerate(account_dirs) 
                                if os.path.basename(d) == last_account), -1)
                if last_index >= 0:
                    next_index = (last_index + 1) % len(account_dirs)
                    return account_dirs[next_index]
    except Exception as e:
        logger.error(f"Error reading last run file: {e}")
    return account_dirs[0] if account_dirs else None

async def enhance_caption_with_gemini(account_name, original_caption):
    if not GEMINI_API_KEY:
        logger.info(f"[{account_name}] Gemini API not configured, using original caption")
        return original_caption
    try:
        parts = original_caption.split("\n\n")
        caption_text = parts[0] if parts else ""
        original_hashtags = parts[1].split() if len(parts) > 1 else []
        prompt = f"""
        Enhance this Instagram caption while keeping the original meaning:
        1. Slightly reword the caption
        2. Add/shuffle emojis if appropriate
        3. Replace 1-3 hashtags with similar ones
        4. Keep similar length
        
        Original caption: {caption_text}
        Original hashtags: {" ".join(original_hashtags)}
        
        Return in this format:
        [rewritten caption]
        
        [new hashtags]
        """
        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.7, "topP": 0.9, "maxOutputTokens": 2000}
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(GEMINI_API_URL, headers=GEMINI_HEADERS, json=data, timeout=10) as response:
                    if response.status != 200:
                        error = await response.text()
                        logger.error(f"[{account_name}] Gemini API error: {error}")
                        return original_caption
                    result = await response.json()
                    if "candidates" not in result or not result["candidates"]:
                        logger.error(f"[{account_name}] No candidates in Gemini response")
                        return original_caption
                    response_text = result["candidates"][0]["content"]["parts"][0]["text"]
                    enhanced_parts = response_text.split("\n\n")
                    if len(enhanced_parts) < 2:
                        logger.warning(f"[{account_name}] Unexpected Gemini response format")
                        return original_caption
                    enhanced_caption = enhanced_parts[0].strip()
                    enhanced_hashtags = enhanced_parts[1].strip()
                    logger.info(f"[{account_name}] Successfully enhanced caption with Gemini")
                    return f"{enhanced_caption}\n\n{enhanced_hashtags}"
            except asyncio.TimeoutError:
                logger.warning(f"[{account_name}] Gemini API timeout")
                return original_caption
            except Exception as e:
                logger.error(f"[{account_name}] Gemini API call failed: {e}")
                return original_caption
    except Exception as e:
        logger.error(f"[{account_name}] Gemini enhancement failed: {e}")
        return original_caption

async def extract_caption(page, account_name):
    logger.info(f"[{account_name}] Extracting caption")
    try:
        spans = await page.query_selector_all(CAPTION_SPAN_SELECTOR)
        best_text = ""
        best_score = 0
        for span in spans:
            try:
                html = await span.inner_html()
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator="\n").strip()
                hashtags = [a.text for a in soup.find_all("a") if a.text.startswith("#")]
                score = len(text) + 10 * len(hashtags)
                if score > best_score:
                    best_score = score
                    best_text = text + "\n\n" + " ".join(hashtags)
            except Exception as e:
                logger.warning(f"[{account_name}] Caption extraction error: {e}")
        if "Liked by" in best_text:
            best_text = "here is video of the day"
            logger.info(f"[{account_name}] 'Liked by' found in caption. Using custom caption.")
        elif "Contact Uploading & Non-Users" in best_text:
            best_text = "here is video of the day"
            logger.info(f"[{account_name}] 'Contact Uploading & Non-Users' found in caption. Using custom caption.")
        elif not best_text:
            best_text = "🔥 Reel of the day!\n\n#reels #viral"
            logger.info(f"[{account_name}] No caption found. Using default caption.")
        enhanced_text = await enhance_caption_with_gemini(account_name, best_text)
        caption_path = os.path.join(BASE_DIR, f"caption_{account_name}.txt")
        with open(caption_path, "w", encoding="utf-8") as f:
            f.write(enhanced_text)
        return enhanced_text
    except Exception as e:
        logger.error(f"[{account_name}] Caption extraction failed: {e}")
        return "🔥 Reel of the day!\n\n#reels #viral"

def clean_caption_file(account_name):
    caption_path = os.path.join(BASE_DIR, f"caption_{account_name}.txt")
    try:
        with open(caption_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        logger.warning(f"[{account_name}] Caption file not found")
        return
    final_lines = []
    seen_hashtags = set()
    for line in lines:
        line = line.strip()
        underscore = False
        if not line or line.lower() == "verified":
            continue
        if line.lower().endswith(("d", "w", "h")):
            continue
        if "@" in line:
            continue
        if "_" in line:
            underscore = True
            continue
        if line.lower() == line and not line.startswith("#") and len(line.split()) == 1:
            continue
        if line.startswith("#"):
            hashtags = line.split()
            for tag in hashtags:
                if tag not in seen_hashtags:
                    seen_hashtags.add(tag)
        elif not underscore:
            final_lines.append(line)
    if seen_hashtags:
        if final_lines and final_lines[-1] != "":
            final_lines.append("")
        final_lines.append(" ".join(sorted(seen_hashtags)))
    with open(caption_path, "w", encoding="utf-8") as f:
        f.write("\n".join(final_lines))
    logger.info(f"[{account_name}] Caption cleaned and formatted")

async def process_video_with_ffmpeg(account_name):
    logger.info(f"[{account_name}] Processing video with FFmpeg")
    try:
        account_dir = os.path.join(COMBO_DIR, account_name)
        config = load_account_config(account_dir)
        if not config:
            return False
        input_path = os.path.join(BASE_DIR, f"reel_{account_name}.mp4")
        output_path = os.path.join(BASE_DIR, f"reel_{account_name}_processed.mp4")
        username = config["username"]
        
        overlay_position = random.choice(["20:H-h-20", "W-w-20:H-h-20", "20:20", "W-w-20:20"])
        text_position = random.choice(["x=20:y=20", "x=w-tw-20:y=20", "x=20:y=h-th-20", "x=w-tw-20:y=h-th-20"])
        center_text_position = f"x=(w-tw)/2+{random.randint(-50, 50)}:y=(h-th)/2+{random.randint(-50, 50)}"
        username_font_color = random.choice(["white", "yellow", "lightblue", "orange", "cyan"])
        watermark_font_color = "white@0.03"
        username_font_size = random.randint(36, 39)
        watermark_font_size = random.randint(13, 16)
        logo_scale = random.uniform(0.19, 0.23)

        logo_width = int(720 * logo_scale)

        # -- Optional Top Title from custom.json --
        video_title = config.get("tittle", "").strip()
        if video_title:
            video_title = video_title.replace("'", "\\'")
            title_font_size = 40
            title_font_color = "white"
            title_text_position = "x=(w-text_w)/2:y=20"
            title_text_cmd = (
            f"drawtext=text='{video_title}':fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
            f"fontsize={title_font_size}:fontcolor={title_font_color}:{title_text_position}:"
            "shadowcolor=black@0.7:shadowx=2:shadowy=2,"
            )
        else:
            title_text_cmd = ""
        ffmpeg_command = [
            'ffmpeg',
            '-i', input_path,
            '-i', os.path.join(BASE_DIR, 'logo.png'),
            '-filter_complex',
            (
            f"[1:v]scale={logo_width}:-1,format=rgba[logo];"
            f"[0:v]scale=iw*1.02:ih*1.02,crop=iw-20:ih-20,{title_text_cmd}"
            "eq=brightness=0.02:saturation=1.3:contrast=1.1,"
            f"drawtext=text='@{username}':fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
            f"fontsize={username_font_size}:fontcolor={username_font_color}:{text_position}:"
            "shadowcolor=black@0.5:shadowx=2:shadowy=2,"
            f"drawtext=text='@{username}':fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:"
            f"fontsize={watermark_font_size}:fontcolor={watermark_font_color}:{center_text_position}:"
            "shadowcolor=black@0.3:shadowx=1:shadowy=1[vt];"
            f"[vt][logo]overlay={overlay_position}:format=auto[v];"
            "[0:a]adelay=200|200,asetrate=44100*1.02,aresample=44100,atempo=1.01,volume=1.1[a]"
            ),
            '-map', '[v]',
            '-map', '[a]',
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-crf', '28',
            '-threads', '2',
            '-c:a', 'aac',
            '-b:a', '96k',
            '-movflags', '+faststart',
            '-y',
            output_path
        ]

        process = await asyncio.create_subprocess_exec(
            *ffmpeg_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            logger.error(f"[{account_name}] FFmpeg processing failed: {error_msg}")
            send_telegram(f"❌ FFmpeg failed for {account_name}: {error_msg}", account_name)
            return False
        logger.info(f"[{account_name}] Video processed successfully")
        return True
    except Exception as e:
        logger.error(f"[{account_name}] FFmpeg error: {e}")
        send_telegram(f"❌ FFmpeg exception for {account_name}: {e}", account_name)
        return False
    finally:
        gc.collect()

async def download_reel_video(account_name, reel_url):
    logger.info(f"[{account_name}] Downloading reel video")
    async with async_playwright() as p:
        browser = None
        try:
            browser = await safe_browser_launch(p.chromium, account_name, headless=False)
            if not browser:
                return False
                
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            )
            account_dir = os.path.join(COMBO_DIR, account_name)
            config = load_account_config(account_dir)
            if not config:
                return False
            await context.add_cookies(config["cookies"])
            page = await context.new_page()
            video_urls = []

            def capture_response(response):
                if ".mp4" in response.url and response.url.startswith("https://") and "bytestart" not in response.url:
                    try:
                        content_length = int(response.headers.get("content-length", 0))
                        if content_length > 100_000:
                            video_urls.append({"url": response.url, "headers": response.request.headers, "size": content_length})
                    except ValueError:
                        pass

            page.on("response", capture_response)
            
            if not await safe_goto(page, reel_url, account_name):
                return False
                
            await save_screenshot(page, "reel_page", account_name)
            try:
                video_element = await page.query_selector("video")
                if video_element:
                    await video_element.click()
                    logger.info(f"[{account_name}] Clicked video element to trigger playback")
            except Exception as e:
                logger.warning(f"[{account_name}] Could not click video element: {e}")
                
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 200)")
                await page.wait_for_timeout(random.randint(2000, 5000))
                await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
                await page.wait_for_timeout(random.randint(500, 1500))
                
            await page.wait_for_timeout(15000)
            
            if not video_urls:
                logger.error(f"[{account_name}] No valid .mp4 URLs found in network responses")
                send_telegram(f"❌ No video URLs found for {account_name}", account_name)
                return False
                
            selected_video = max(video_urls, key=lambda x: x["size"]) if video_urls else None
            if not selected_video:
                logger.error(f"[{account_name}] No valid video URL selected")
                send_telegram(f"❌ No valid video URL selected for {account_name}", account_name)
                return False
                
            selected_url = selected_video["url"]
            logger.info(f"[{account_name}] Selected video URL: {selected_url}")
            output_path = os.path.join(BASE_DIR, f"reel_{account_name}.mp4")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
                "Accept": "video/mp4",
                "Accept-Encoding": "identity",
                "Referer": reel_url,
                "Origin": "https://www.instagram.com"
            }
            
            async def download_operation():
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(selected_url, timeout=90) as response:
                        if response.status != 200:
                            raise Exception(f"HTTP status {response.status}")
                        with open(output_path, "wb") as f:
                            total_size = 0
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)
                                total_size += len(chunk)
                        if total_size < 100_000:
                            raise Exception(f"File too small: {total_size} bytes")
                        return True
            
            download_success = await execute_with_retries(
                account_name,
                download_operation,
                max_retries=3,
                delay=5,
                operation_name="video download"
            )
            
            if not download_success:
                return False
                
            probe_cmd = ["ffprobe", "-v", "error", "-show_streams", "-of", "json", output_path]
            probe_proc = await asyncio.create_subprocess_exec(
                *probe_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            probe_stdout, probe_stderr = await probe_proc.communicate()
            if probe_proc.returncode != 0:
                logger.error(f"[{account_name}] FFmpeg probe failed: {probe_stderr.decode().strip()}")
                send_telegram(f"❌ Invalid video file for {account_name}: FFmpeg probe failed", account_name)
                return False
                
            probe_data = json.loads(probe_stdout)
            if not probe_data.get("streams"):
                logger.error(f"[{account_name}] No valid streams found in video file")
                send_telegram(f"❌ Invalid video file for {account_name}: No streams", account_name)
                return False
                
            logger.info(f"[{account_name}] Video downloaded and verified successfully")
            return True
            
        except Exception as e:
            logger.error(f"[{account_name}] Video download error: {e}")
            send_telegram(f"❌ Video download error for {account_name}: {e}", account_name)
            return False
        finally:
            if browser:
                await browser.close()
            gc.collect()

async def check_login_page(page, account_name):
    try:
        login_elements = [
            'input[name="username"]',
            'input[name="password"]',
            'button:has-text("Log In")',
            'text="Log in to Instagram"',
            'text="Log in with Facebook"',
            'text="Forgot password?"'
        ]
        for selector in login_elements:
            if await page.query_selector(selector):
                logger.warning(f"[{account_name}] Detected login page - cookies may be expired")
                send_telegram(f"⚠️ LOGIN PAGE DETECTED - COOKIES EXPIRED for {account_name}", account_name)
                await save_screenshot(page, "login_page_detected", account_name)
                return True
        not_now_btn = await page.query_selector('text="Not Now"')
        if not_now_btn:
            logger.info(f"[{account_name}] Found 'Not Now' button, dismissing it")
            await not_now_btn.click()
            await page.wait_for_timeout(1000)
        return False
    except Exception as e:
        logger.error(f"[{account_name}] Error checking login page: {e}")
        return False

async def simulate_human_activity(page, account_name):
    logger.info(f"[{account_name}] Simulating human activity")
    try:
        for _ in range(random.randint(1, 3)):
            scroll_distance = random.randint(200, 600)
            await page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            await page.wait_for_timeout(random.randint(1000, 3000))
        await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
        await page.wait_for_timeout(random.randint(500, 1500))
        try:
            elements = await page.query_selector_all('div[role="presentation"]')
            if elements:
                random_element = random.choice(elements)
                await random_element.click()
                await page.wait_for_timeout(random.randint(1000, 2000))
        except Exception as e:
            logger.warning(f"[{account_name}] Error during random click: {e}")
        await page.wait_for_timeout(random.randint(10000, 20000))
        logger.info(f"[{account_name}] Human activity simulation completed")
    except Exception as e:
        logger.error(f"[{account_name}] Error simulating human activity: {e}")

async def upload_reel_to_instagram(account_config):
    account_name = account_config["account_name"]
    logger.info(f"[{account_name}] Starting Instagram upload")
    p_firefox_context = None
    firefox_browser = None
    try:
        p_firefox_context = await async_playwright().start()
        firefox_browser = await safe_browser_launch(p_firefox_context.firefox, account_name, headless=False)
        if not firefox_browser:
            return False
            
        firefox_context = await firefox_browser.new_context(viewport={"width": 375, "height": 812})
        await firefox_context.add_cookies(account_config["cookies"])
        ff_page = await firefox_context.new_page()
        
        if not await safe_goto(ff_page, "https://www.instagram.com/", account_name):
            return False
            
        await save_screenshot(ff_page, "instagram_home", account_name)
        if await check_login_page(ff_page, account_name):
            return False
            
        await simulate_human_activity(ff_page, account_name)
        
        try:
            await ff_page.click('[aria-label="New post"]')
            await ff_page.wait_for_timeout(1000)
            
            await ff_page.evaluate("""() => {
                [...document.querySelectorAll('div')].find(el => el.textContent.trim() === 'Post')?.click();
            }""")
            await ff_page.wait_for_timeout(2000)
            await save_screenshot(ff_page, "clicked_post", account_name)
            
            try:
                await ff_page.evaluate("""() => {
                    const input = document.querySelector('input[type="file"]');
                    if (input) input.style.display = 'block';
                }""")
            except Exception as e:
                logger.error(f"[{account_name}] Error making file input visible: {e}")
                send_telegram(f"❌ Error in making file input visible: {e}", account_name)
                return False
                
            await ff_page.wait_for_timeout(1000)
            
            try:
                input_file = await ff_page.query_selector('input[type="file"]')
                if input_file:
                    video_path = os.path.join(BASE_DIR, f"reel_{account_name}_processed.mp4")
                    await input_file.set_input_files(video_path)
                    logger.info(f"[{account_name}] Video uploaded to file input")
                else:
                    raise Exception("File input element not found.")
            except Exception as e:
                logger.error(f"[{account_name}] File upload failed: {e}")
                send_telegram(f"❌ File upload failed during Instagram post: {e}", account_name)
                return False
                
            await ff_page.wait_for_timeout(5000)
            await save_screenshot(ff_page, "file_uploaded", account_name)
            
            try:
                await ff_page.locator("video").first.wait_for(timeout=15000)
                logger.info(f"[{account_name}] Video preview loaded")
            except PlaywrightTimeoutError:
                logger.warning(f"[{account_name}] Preview video element not detected within timeout, continuing anyway.")
            except Exception as e:
                logger.warning(f"[{account_name}] Error waiting for video preview: {e}, continuing anyway.")
                
            try:
                await ff_page.evaluate("""() => {
                    const okBtn = [...document.querySelectorAll("button")].find(b => b.textContent.trim() === "OK");
                    if (okBtn) okBtn.click();
                }""")
                logger.info(f"[{account_name}] OK popup clicked")
            except Exception as e:
                logger.warning(f"[{account_name}] OK popup not shown or clickable: {e}")
                
            await save_screenshot(ff_page, "ok_popup", account_name)
            
            try:
                crop_button = ff_page.locator('svg[aria-label="Select crop"]')
                await crop_button.scroll_into_view_if_needed()
                await crop_button.wait_for(state="visible", timeout=6000)
                await crop_button.click()
                logger.info(f"[{account_name}] Crop button clicked")
                await ff_page.wait_for_timeout(1000)
                await save_screenshot(ff_page, "crop_button_clicked", account_name)
                
                original_button = ff_page.locator('svg[aria-label="Photo outline icon"]')
                await original_button.scroll_into_view_if_needed()
                await original_button.wait_for(state="visible", timeout=6000)
                await original_button.click()
                logger.info(f"[{account_name}] Original button clicked")
                await ff_page.wait_for_timeout(1000)
                await save_screenshot(ff_page, "original_button_clicked", account_name)
            except Exception as e:
                logger.warning(f"[{account_name}] Failed to click Crop or Original buttons: {e}")
                send_telegram(f"⚠️ Crop/Original buttons failed for {account_name}: {e}", account_name)
                
            try:
                next_btn = ff_page.locator('div[role="button"]:has-text("Next")')
                await next_btn.wait_for(state="visible", timeout=20000)
                await next_btn.click()
                await ff_page.wait_for_timeout(3000)
                await save_screenshot(ff_page, "next1", account_name)
                
                await next_btn.click()
                await ff_page.wait_for_timeout(3000)
                await save_screenshot(ff_page, "next2", account_name)
            except Exception as e:
                logger.error(f"[{account_name}] Next buttons failed: {e}")
                send_telegram(f"❌ Failed to click Next buttons for {account_name}: {e}", account_name)
                return False
                
            try:
                if account_config["use_custom_caption"]:
                    caption_text = account_config["custom_caption"]
                else:
                    caption_path = os.path.join(BASE_DIR, f"caption_{account_name}.txt")
                    with open(caption_path, "r", encoding="utf-8") as f:
                        caption_text = f.read()
                hashtags = account_config["hashtags"]
                if hashtags:
                    num_hashtags = min(10, len(hashtags))
                    selected_hashtags = random.sample(hashtags, num_hashtags)
                    hashtag_string = " ".join(selected_hashtags)
                    full_caption = f"{caption_text.strip()}\n\n{hashtag_string} .."
                else:
                    full_caption = caption_text.strip()
                    
                await ff_page.locator('[aria-label="Write a caption..."]').click()
                await ff_page.keyboard.type(full_caption)
                logger.info(f"[{account_name}] Caption and hashtags added")
            except Exception as e:
                logger.error(f"[{account_name}] Caption and hashtag add failed: {e}")
                send_telegram(f"❌ Failed to add caption/hashtags for {account_name}: {e}", account_name)
                return False
                
            await save_screenshot(ff_page, "caption_and_hashtags_added", account_name)
            
            try:
                share_button_selector = "body > div.x1n2onr6.xzkaem6 > div.x9f619.x1n2onr6.x1ja2u2z > div > div.x1uvtmcs.x4k7w5x.x1h91t0o.x1beo9mf.xaigb6o.x12ejxvf.x3igimt.xarpa2k.xedcshv.x1lytzrv.x1t2pt76.x7ja8zs.x1n2onr6.x1qrby5j.x1jfb8zj > div > div > div > div > div > div > div > div._ap97 > div > div > div > div._ac7b._ac7d > div > div"
                await ff_page.wait_for_selector(share_button_selector, state="visible", timeout=9000)
                await ff_page.click(share_button_selector)
                logger.info(f"[{account_name}] Share button clicked successfully")
                send_telegram(f"✅ Posted reel successfully for {account_config['username']}", account_name)
                await ff_page.wait_for_timeout(36000)
                await save_screenshot(ff_page, "shared_clicked", account_name)
                logger.info(f"[{account_name}] Closing browser")
                await ff_page.wait_for_timeout(1000)
                return True
            except PlaywrightTimeoutError:
                logger.error(f"[{account_name}] Share button not found or not clickable within timeout")
                send_telegram(f"❌ Failed to post for {account_name}: Share button not found", account_name)
                return False
            except Exception as e:
                logger.error(f"[{account_name}] Share click failed: {e}")
                send_telegram(f"❌ Failed to post for {account_name}: Share click failed: {e}", account_name)
                return False
        except Exception as e:
            logger.error(f"[{account_name}] Error during Instagram upload flow: {e}")
            send_telegram(f"❌ Error during upload for {account_name}: {e}", account_name)
            return False
    except Exception as e:
        logger.error(f"[{account_name}] Unexpected error during Firefox phase: {e}")
        send_telegram(f"❌ Unexpected error for {account_name}: {e}", account_name)
        return False
    finally:
        if firefox_browser:
            await firefox_browser.close()
        if p_firefox_context:
            await p_firefox_context.stop()
        logger.info(f"[{account_name}] Closed Firefox browser")
        gc.collect()

async def process_single_account(account_dir):
    gc.collect()
    account_name = os.path.basename(account_dir)
    logger.info(f"Starting processing for account: {account_name}")
    account_config = load_account_config(account_dir)
    username = account_config.get("username", "@unknown") if account_config else "@unknown"
    send_telegram(f"Starting processing 🚀 for {username}", account_name)
    try:
        if not account_config:
            logger.error(f"[{account_name}] Account config load failed, skipping")
            return False
        clean_temp_files(account_name)
        reel_url = None
        
        # Phase 1: Get reel URL
        async with async_playwright() as p:
            browser = None
            try:
                browser = await safe_browser_launch(p.chromium, account_name, headless=False)
                if not browser:
                    return False
                    
                context = await browser.new_context()
                await context.add_cookies(account_config["cookies"])
                page = await context.new_page()
                
                if not await safe_goto(page, REELS_URL, account_name):
                    return False
                    
                await save_screenshot(page, "reels_page", account_name)
                if await check_login_page(page, account_name):
                    return False
                    
                try:
                    await page.wait_for_selector("video", timeout=10000)
                except Exception as e:
                    logger.error(f"[{account_name}] No videos found on reels page: {e}")
                    send_telegram(f"❌ No videos found for {account_name}", account_name)
                    return False
                    
                videos = await page.query_selector_all("video")
                logger.info(f"[{account_name}] Found {len(videos)} reels")
                reel_url = page.url.replace("/reels/", "/reel/")
                logger.info(f"[{account_name}] Found reel URL: {reel_url}")
            except Exception as e:
                logger.error(f"[{account_name}] Reel URL fetch error: {e}")
                send_telegram(f"❌ Reel URL fetch failed for {account_name}: {e}", account_name)
                return False
            finally:
                if browser:
                    await browser.close()
                gc.collect()
                
        if not reel_url:
            return False
            
        # Phase 2: Download reel
        download_success = await execute_with_retries(
            account_name,
            lambda: download_reel_video(account_name, reel_url),
            max_retries=2,
            delay=10,
            operation_name="reel download"
        )
        
        if not download_success:
            return False
            
        # Phase 3: Extract caption
        async with async_playwright() as p:
            browser = None
            try:
                browser = await safe_browser_launch(p.firefox, account_name, headless=False)
                if not browser:
                    return False
                    
                context = await browser.new_context()
                await context.add_cookies(account_config["cookies"])
                page = await context.new_page()
                
                if not await safe_goto(page, reel_url, account_name):
                    return False
                    
                await save_screenshot(page, "reel_for_caption", account_name)
                if await check_login_page(page, account_name):
                    return False
                    
                await extract_caption(page, account_name)
                clean_caption_file(account_name)
            except Exception as e:
                logger.error(f"[{account_name}] Caption extraction error: {e}")
                send_telegram(f"❌ Caption extraction failed for {account_name}: {e}", account_name)
                return False
            finally:
                if browser:
                    await browser.close()
                gc.collect()
                
        # Phase 4: Process video
        process_success = await execute_with_retries(
            account_name,
            lambda: process_video_with_ffmpeg(account_name),
            max_retries=2,
            delay=10,
            operation_name="video processing"
        )
        
        if not process_success:
            return False
            
        # Phase 5: Upload to Instagram
        upload_success = await execute_with_retries(
            account_name,
            lambda: upload_reel_to_instagram(account_config),
            max_retries=2,
            delay=15,
            operation_name="Instagram upload"
        )
        
        return upload_success
    except Exception as e:
        logger.error(f"[{account_name}] Unhandled error in account processing: {e}")
        send_telegram(f"🚨 Unhandled error for {account_name}: {e}", account_name)
        return False
    finally:
        clean_temp_files(account_name)
        truncate_log_file()
        save_last_run(account_name)
        gc.collect()

async def main_loop():
    logger.info("Starting main automation loop")
    send_telegram("🤖 Instagram Automation Bot Started")
    while True:
        try:
            update_healthcheck()
            
            account_dirs = [
                os.path.join(COMBO_DIR, d)
                for d in sorted(os.listdir(COMBO_DIR))
                if os.path.isdir(os.path.join(COMBO_DIR, d))
            ]
            if not account_dirs:
                logger.error("No accounts found in combo directory")
                send_telegram("❌ No accounts configured in ~/ui/combo/")
                await asyncio.sleep(300)
                continue
                
            next_account = get_next_account(account_dirs)
            if not next_account:
                logger.error("No valid next account found")
                await asyncio.sleep(300)
                continue
                
            account_name = os.path.basename(next_account)
            start_time = datetime.now()
            
            try:
                success = await memory_safe_operation(
                    account_name,
                    lambda: process_single_account(next_account),
                    "account processing"
                )
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds() / 60
                if success:
                    logger.info(f"[{account_name}] Completed successfully in {duration:.1f} minutes")
                else:
                    logger.error(f"[{account_name}] Failed after {duration:.1f} minutes")
            except Exception as e:
                logger.error(f"[{account_name}] Critical error in account processing: {e}")
                send_telegram(f"🚨 Critical error for {account_name}: {e}", account_name)
                
            cycle_delay = random.randint(MIN_CYCLE_DELAY, MAX_CYCLE_DELAY)
            logger.info(f"Completed account {account_name}. Waiting {cycle_delay//60} minutes before next cycle")
            send_telegram(f"Next cycle in ⏳ {cycle_delay} seconds")
            await asyncio.sleep(cycle_delay)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            send_telegram(f"🚨 FATAL ERROR in main loop: {e}")
            await asyncio.sleep(60)
        finally:
            gc.collect()

async def run_bot_with_restart():
    while True:
        try:
            await main_loop()
        except KeyboardInterrupt:
            logger.info("Script stopped by user")
            send_telegram("🛑 Script stopped by user")
            break
        except Exception as e:
            logger.error(f"Bot crashed with error: {e}")
            send_telegram(f"🚨 BOT CRASHED: {e}\nRestarting in 60 seconds..")
            await asyncio.sleep(60)
            continue

if __name__ == "__main__":
    try:
        logger.info("Starting bot process")
        send_telegram("🤖 Starting Instagram Automation Bot")
        asyncio.run(run_bot_with_restart())
    except Exception as e:
        logger.error(f"Fatal startup error: {e}")
        send_telegram(f"🚨 FATAL STARTUP ERROR: {e}")