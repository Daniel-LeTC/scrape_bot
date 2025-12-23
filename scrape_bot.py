import argparse
import calendar
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import polars as pl
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

import config
from modern_etl import RawToSilverIngester, ETLLogger

# Load environment variables from .env file
load_dotenv()

class AutoLogin:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.login_url = config.LOGIN_URL
        self.target_url = config.DASHBOARD_URL

    def get_token(self):
        """
        Automates login process using Playwright to retrieve authentication token.
        Returns the token string if successful, None otherwise.
        """
        if not self.username or not self.password:
            print("Error: PPC_USER or PPC_PASS not found in environment variables.")
            return None

        print(f"Attempting login for user: {self.username}...")
        
        with sync_playwright() as p:
            browser = None
            try:
                # Launch browser in headless mode with stealth arguments
                browser = p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"]
                )
                
                # Create context with a standard user agent
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()

                # Navigate to login page and wait for network to settle
                page.goto(self.login_url, wait_until="networkidle")
                
                # Extra wait for JS/Hydration
                page.wait_for_timeout(2000)

                # Wait for specific IDs provided by user
                try:
                    page.wait_for_selector('#username', timeout=15000)
                except Exception:
                    # Capture screenshot on failure for debugging
                    debug_path = os.path.join(config.OUTPUT_DIR, "debug_login_error.png")
                    page.screenshot(path=debug_path)
                    print(f"Error: Timeout waiting for login fields. Screenshot saved to {debug_path}")
                    return None
                
                # Input credentials using exact IDs
                page.fill('#username', self.username)
                page.fill('#password', self.password)
                
                # Submit form using type="submit"
                page.click('button[type="submit"]')
                
                # Wait for navigation away from login page
                print("Waiting for redirect...")
                # Wait until URL does NOT contain 'signin' anymore
                page.wait_for_function("!window.location.href.includes('signin')", timeout=30000)
                
                # Wait a bit for LocalStorage to be populated
                page.wait_for_timeout(2000)
                print("Login successful (Redirected).")

                # Retrieve token from Local Storage
                storage = page.evaluate("() => JSON.stringify(window.localStorage)")
                storage_dict = json.loads(storage)
                
                token = storage_dict.get("access_token") or \
                        storage_dict.get("token") or \
                        storage_dict.get("auth_token")
                
                # Fallback: Retrieve token from Cookies
                if not token:
                    cookies = context.cookies()
                    for c in cookies:
                        if "token" in c['name'].lower():
                            token = c['value']
                            break
                
                return token
            except Exception as e:
                print(f"Login failed: {str(e)}")
                return None
            finally:
                if browser:
                    browser.close()


class TokenStealer:
    @staticmethod
    def get_windows_clipboard():
        """Retrieves clipboard content using PowerShell."""
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", "Get-Clipboard"],
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except Exception:
            return ""

    @staticmethod
    def wait_for_token(timeout=60):
        """Waits for a JWT token to appear in the clipboard."""
        print("Waiting for token in clipboard (Manual Fallback)...")
        start_time = time.time()
        jwt_pattern = r"eyJ[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+\.[a-zA-Z0-9-_]+"
        last_clip = ""

        while (time.time() - start_time) < timeout:
            current_clip = TokenStealer.get_windows_clipboard()
            if current_clip != last_clip:
                match = re.search(jwt_pattern, current_clip)
                if match:
                    token = match.group(0)
                    print("Token detected in clipboard.")
                    return token
                last_clip = current_clip
            time.sleep(1)
        return None


class PPCHarvester:
    """
    Worker 1: Chuyên trách việc cào dữ liệu từ Web UI/API của PPC Tool hiện tại.
    Output: Đẩy thẳng vào Ingester để đóng dấu & lưu Parquet.
    """
    def __init__(self, token, logger=None):
        self.headers = config.get_headers(token)
        self.logger = logger or ETLLogger()
        # Initialize the Modern Ingester
        self.ingester = RawToSilverIngester(logger=self.logger)

    def fetch_data(self, start_date_str, end_date_str, step="day", dry_run=False, debug=False):
        """
        Iterates through date range based on granularity (step) and downloads reports.
        step: 'day', 'month', 'year', 'total'
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        current = start_date
        request_count = 0

        print(f"🚀 START HARVEST. Range: {start_date_str} to {end_date_str}. Step: {step}")
        if dry_run:
            print("⚠️ WARNING: DRY-RUN MODE. No HTTP requests will be sent.")

        while current <= end_date:
            request_count += 1
            
            # Determine chunk end date based on step
            if step == "day":
                chunk_end = current
            elif step == "month":
                _, last_day = calendar.monthrange(current.year, current.month)
                chunk_end = current.replace(day=last_day)
            elif step == "year":
                chunk_end = current.replace(month=12, day=31)
            else: # total
                chunk_end = end_date

            # Clamp chunk_end to global end_date
            if chunk_end > end_date:
                chunk_end = end_date
            
            # Format dates for API
            c_start_iso = current.strftime("%Y-%m-%d")
            c_end_iso = chunk_end.strftime("%Y-%m-%d")
            
            print(f"[{request_count}] Processing range: {c_start_iso} to {c_end_iso}")

            params = {
                "page": 1,
                "period": "custom",
                "timeFrame": "custom",
                "fromDate": f"{c_start_iso}T00:00:00.000Z",
                "toDate": f"{c_end_iso}T23:59:59.000Z",
                "flag": 1,
                "fields": config.DEFAULT_FIELDS,
            }

            if debug:
                print(f"   [DEBUG] Params: {params}")

            if dry_run:
                print(f"   [DRY-RUN] Would fetch and ingest: {c_start_iso} - {c_end_iso}")
            else:
                try:
                    response = requests.get(
                        config.API_BASE_URL, headers=self.headers, params=params, timeout=60
                    )

                    if response.status_code == 200:
                        # 1. Save Raw File (Audit Trail)
                        xlsx_filename = f"raw_ppc_{c_start_iso}_{c_end_iso}.xlsx"
                        xlsx_path = os.path.join(config.RAW_DATA_DIR, xlsx_filename)
                        
                        with open(xlsx_path, "wb") as f:
                            f.write(response.content)
                        
                        # 2. Ingest to Silver Layer (Modern Logic)
                        metadata = {
                            "start_date": c_start_iso,
                            "end_date": c_end_iso,
                            "source_type": "api_harvest",
                            "step": step
                        }
                        # Calling the modern ingester!
                        result_path = self.ingester.ingest_file(xlsx_path, metadata)
                        
                        if result_path:
                            print(f"   ✅ Ingested: {os.path.basename(result_path)}")
                        else:
                            print(f"   ❌ Ingest Failed for {xlsx_filename}")

                    elif response.status_code == 401:
                        print("   ❌ Token expired during fetch!")
                        self.logger.log_error("Fetch", "API", "Token Expired")
                        return False
                    else:
                        print(f"   ❌ Error: Status Code {response.status_code}")

                except Exception as e:
                    print(f"   Exception: {str(e)}")
                    self.logger.log_error("Fetch", "API", e)

            # Advance current date
            if step == "total":
                break 
            
            current = chunk_end + timedelta(days=1)
            if not dry_run:
                time.sleep(1) 

        return True

class DBSourceFetcher:
    """
    [SKELETON] Worker 2: Chuyên trách việc lấy data từ Database cũ (SQL hoặc API Wrapper).
    Output: Đẩy thẳng vào Ingester (dùng ingest_memory_data).
    """
    def __init__(self, connection_string=None, api_url=None, logger=None):
        self.logger = logger or ETLLogger()
        self.ingester = RawToSilverIngester(logger=self.logger)
        self.conn_str = connection_string
        self.api_url = api_url

    def fetch_from_sql(self, query):
        """
        Scenario A: Direct DB Access.
        Uses connectorx or sqlalchemy to read SQL -> DataFrame -> Ingest Memory.
        """
        # TODO: import connectorx as cx
        # df = cx.read_sql(self.conn_str, query)
        # metadata = {"source": "sql_db", "end_date": "detect_from_data"}
        # self.ingester.ingest_memory_data(df.to_dicts(), metadata)
        pass

    def fetch_from_api_wrapper(self, endpoint, payload):
        """
        Scenario B: API Access to DB.
        Requests -> JSON -> Ingest Memory.
        """
        # TODO: resp = requests.post(self.api_url + endpoint, json=payload)
        # data = resp.json()
        # self.ingester.ingest_memory_data(data, metadata)
        pass

def main():
    parser = argparse.ArgumentParser(description="PPC Scraper & Ingester (Modern Architecture)")
    parser.add_argument("--start", help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End Date (YYYY-MM-DD)")
    parser.add_argument("--step", choices=["day", "month", "year", "total"], default="day", help="Aggregation Granularity")
    parser.add_argument("--mode", choices=["full", "offline"], default="full", help="Operation Mode")
    parser.add_argument("--dry-run", action="store_true", help="Simulate run without making API requests")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()

    # Get Credentials
    user = os.getenv("PPC_USER")
    password = os.getenv("PPC_PASS")
    token = None

    if args.mode == "full":
        # 1. Attempt Auto Login
        if args.dry_run:
             print("Info: DRY-RUN enabled. Skipping login authentication (using mock token).")
             token = "mock_token_dry_run"
        else:
            login_bot = AutoLogin(user, password)
            token = login_bot.get_token()

            # 2. Fallback to Clipboard
            if not token:
                token = TokenStealer.wait_for_token(timeout=30)

        if not token:
            print("Failed to retrieve token. Exiting.")
            return

        # 3. Fetch Data (Using New Logic)
        start_date = args.start or input("Start Date (YYYY-MM-DD): ").strip()
        end_date = args.end or input("End Date (YYYY-MM-DD): ").strip()
        step = args.step

        harvester = PPCHarvester(token)
        harvester.fetch_data(start_date, end_date, step=step, dry_run=args.dry_run, debug=args.debug)

    print("\n🏁 Operation Completed. Check 'silver_data' for results.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Cancelled by user.")
    except Exception as e:
        print(f"System Error: {e}")