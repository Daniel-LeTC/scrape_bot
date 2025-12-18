import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import polars as pl
import requests

import config


# ==========================================
# 0. THE TOKEN STEALER: MÓC TÚI QUA CLIPBOARD
# ==========================================
class TokenStealer:
    @staticmethod
    def get_windows_clipboard():
        """Gọi PowerShell để lấy clipboard từ Windows Host"""
        try:
            # Gọi powershell.exe của Windows từ trong WSL
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
        print("\n🕵️ Đang chờ Token từ Clipboard...")
        print("   👉 Mày qua trình duyệt, bấm cái Bookmarklet 'Steal Token' đi.")
        print("   👉 Hoặc copy Token thủ công (Ctrl+C). Script sẽ tự bắt dính.")

        start_time = time.time()
        # Regex tìm JWT (bắt đầu bằng eyJ...)
        jwt_pattern = r"eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+"

        last_clip = ""

        while (time.time() - start_time) < timeout:
            current_clip = TokenStealer.get_windows_clipboard()

            if current_clip != last_clip:
                match = re.search(jwt_pattern, current_clip)
                if match:
                    token = match.group(0)
                    print("\n✅ BINGO! Đã bắt được Token từ Clipboard.")

                    # --- FIX LỖI SET-CLIPBOARD ---
                    # Thay vì set rỗng, ta set một dấu cách và giấu lỗi đi (stderr=DEVNULL)
                    try:
                        subprocess.run(
                            ["powershell.exe", "-Command", "Set-Clipboard -Value ' '"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    except:
                        pass  # Kệ cha nó nếu không xóa được
                    # -----------------------------

                    return token
                else:
                    preview = (
                        current_clip[:20] + "..."
                        if len(current_clip) > 20
                        else current_clip
                    )
                    if preview:
                        print(f"   Scanning... (Bỏ qua: '{preview}')", end="\r")

                last_clip = current_clip

            time.sleep(1)

        print("\n⏰ Hết thời gian chờ.")
        return None


# ==========================================
# 1. THE HARVESTER: CÀO DATA QUA API
# ==========================================
class PPCHarvester:
    def __init__(self, token):
        self.headers = config.get_headers(token)

    def fetch_daily_data(self, start_date_str, end_date_str, dry_run=False):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        delta = end_date - start_date
        total_days = delta.days + 1

        print("\n📊 KẾ HOẠCH CÀO DỮ LIỆU:")
        print(f"   - Từ ngày: {start_date_str}")
        print(f"   - Đến ngày: {end_date_str}")
        print(f"   - Tổng số Request API: {total_days}")

        if dry_run:
            return

        current_date = start_date
        request_count = 0

        while current_date <= end_date:
            date_iso = current_date.strftime("%Y-%m-%d")
            request_count += 1
            print(f"[{request_count}/{total_days}] 🚀 Đang tải: {date_iso}")

            params = {
                "page": 1,
                "period": "custom",
                "timeFrame": "custom",
                "fromDate": f"{date_iso}T00:00:00.000Z",
                "toDate": f"{date_iso}T23:59:59.000Z",
                "flag": 1,
                "fields": config.DEFAULT_FIELDS,
                "needReorder": "false",
                "isWinSku": "false",
            }

            try:
                response = requests.get(
                    config.API_BASE_URL, headers=self.headers, params=params, timeout=30
                )

                if response.status_code == 200:
                    file_path = os.path.join(
                        config.RAW_DATA_DIR, f"ppc_report_{date_iso}.xlsx"
                    )
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    print(f"   ✅ Saved: {file_path}")
                elif response.status_code == 401:
                    print(
                        "   🔥 Token hết hạn! Copy cái mới vào Clipboard để tiếp tục..."
                    )
                    new_token = TokenStealer.wait_for_token(timeout=300)
                    if new_token:
                        self.headers = config.get_headers(new_token)
                        continue
                    else:
                        break
                else:
                    print(f"   ❌ Error {date_iso}: {response.status_code}")

            except Exception as e:
                print(f"   🔥 Exception {date_iso}: {str(e)}")

            time.sleep(1.5)
            current_date += timedelta(days=1)


# ==========================================
# 2. THE MASTER JOINER (GIỮ NGUYÊN)
# ==========================================
class DataProcessor:
    @staticmethod
    def clean_currency_column(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
        if col_name in df.columns:
            if df.schema[col_name] == pl.String:
                return df.with_columns(
                    pl.col(col_name)
                    .str.replace_all(r"[$,]", "")
                    .str.replace_all(r"%", "")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                )
        return df

    @staticmethod
    def process_and_merge(output_filename="Master_PPC_Data.csv"):
        import glob

        files = glob.glob(os.path.join(config.RAW_DATA_DIR, "*.xlsx"))

        if not files:
            print("⚠️ Không có file raw nào.")
            return None

        lazy_dfs = []
        print(f"📦 Đang xử lý {len(files)} files...")

        # Suppress openpyxl warnings (cho đỡ rác mắt)
        import warnings

        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

        import pandas as pd

        for f in files:
            try:
                date_str = os.path.basename(f).split("_")[2].replace(".xlsx", "")
                temp_pd = pd.read_excel(f, dtype=str)
                temp_df = pl.from_pandas(temp_pd)
                temp_df = temp_df.with_columns(pl.lit(date_str).alias("Report_Date"))
                lazy_dfs.append(temp_df)
            except Exception:
                pass

        if not lazy_dfs:
            return None

        master_df = pl.concat(lazy_dfs)

        numeric_cols = [
            "Unit sold (Actual)",
            "Ads Spend (Actual)",
            "Revenue (Actual)",
            "Refund",
            "FBA Stock",
            "Phase",
        ]

        for col in numeric_cols:
            master_df = DataProcessor.clean_currency_column(master_df, col)

        master_df = master_df.unique(subset=["SKU", "Report_Date"], keep="last").sort(
            ["Report_Date", "SKU"]
        )

        output_path = os.path.join(config.OUTPUT_DIR, output_filename)
        master_df.write_csv(output_path)
        print(f"🏆 Master File: {output_path} ({master_df.height} rows)")
        return master_df


# ==========================================
# 3. THE TASTE SPLITTER (GIỮ NGUYÊN)
# ==========================================
class TasteAnalyzer:
    @staticmethod
    def split_for_departments(df: pl.DataFrame):
        try:
            # Perf Team
            bleeding = df.filter(
                (pl.col("Unit sold (Actual)") == 0)
                & (pl.col("Ads Spend (Actual)") > 30.0)
            )
            bleeding.write_csv(
                os.path.join(config.OUTPUT_DIR, "Perf_Bleeding_Alert.csv")
            )

            # Product Team
            phase3 = df.filter(pl.col("Phase") == 3.0)
            phase3.write_csv(
                os.path.join(config.OUTPUT_DIR, "Product_Phase3_Analysis.csv")
            )

            # Marketing Team
            niche_summary = (
                df.group_by("Main niche")
                .agg(
                    [
                        pl.col("Revenue (Actual)").sum().alias("Total_Revenue"),
                        pl.col("Ads Spend (Actual)").sum().alias("Total_Ads_Spend"),
                        pl.col("Refund").sum().alias("Total_Refund"),
                    ]
                )
                .sort("Total_Revenue", descending=True)
            )
            niche_summary.write_csv(
                os.path.join(config.OUTPUT_DIR, "Marketing_Niche_Summary.csv")
            )
            print("🍱 Đã chẻ xong báo cáo.")
        except Exception as e:
            print(f"⚠️ Lỗi phân tích: {e}")


# ==========================================
# 4. MAIN ENTRY
# ==========================================
if __name__ == "__main__":
    try:
        print("--- PPC TOOL AUTOMATION (CLIPBOARD EDITION) ---")

        print("\nChọn chế độ:")
        print("1. Chạy full (Cào mới + Gộp + Chẻ số)")
        print("2. Chỉ gộp data cũ (Offline)")
        choice = input("👉 (1/2): ").strip()

        if choice == "1":
            token = TokenStealer.wait_for_token()

            if not token:
                print("❌ Không có Token. Bye.")
                sys.exit(0)

            start_date = input("📅 Ngày bắt đầu (YYYY-MM-DD): ").strip()
            end_date = input("📅 Ngày kết thúc (YYYY-MM-DD): ").strip()

            harvester = PPCHarvester(token)
            harvester.fetch_daily_data(start_date, end_date)

        processor = DataProcessor()
        master_data = processor.process_and_merge()

        if master_data is not None:
            analyzer = TasteAnalyzer()
            analyzer.split_for_departments(master_data)
            print("\n✨ Mission Complete!")

    except KeyboardInterrupt:
        print("\n🛑 Cancelled.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        sys.exit(1)
