# PPC Automation Tool (Scraper & Analyzer)

Tool tự động hóa quy trình lấy dữ liệu quảng cáo (PPC), xử lý và chuẩn hóa dữ liệu cho các hệ thống phân tích phía sau.

Hệ thống được thiết kế theo tư duy **ELT (Extract - Load - Transform)**, tối ưu hóa cho dữ liệu lớn bằng **Polars** và **Parquet**, hỗ trợ chạy **Headless** hoàn toàn.

---

## 🛠️ Yêu cầu cài đặt (Prerequisites)

Dự án sử dụng `uv` để quản lý package và `playwright` để giả lập trình duyệt.

1.  **Cài đặt dependencies:**
    ```bash
    uv sync
    ```

2.  **Cài đặt trình duyệt cho Playwright (Bắt buộc cho Auto Login):**
    ```bash
    uv run playwright install chromium
    ```

3.  **Cấu hình bảo mật:**
    *   Tạo file `.env` tại thư mục gốc `scrape_tool/`.
    *   Copy nội dung từ `.env.example` hoặc điền theo mẫu:
        ```env
        PPC_USER=your_username
        PPC_PASS=your_password
        ```

---

## 🚀 Hướng dẫn sử dụng (Usage)

Chạy tool thông qua entry point `main.py`.

### 1. Chạy cơ bản (Lấy data từng ngày)
Chế độ mặc định, phù hợp để lấy dữ liệu chi tiết (Time-series) để vẽ biểu đồ biến động.
```bash
uv run main.py --start 2025-01-01 --end 2025-01-07
```

### 2. Chạy tổng hợp (Aggregated Fetching) - `Recommended`
Sử dụng flag `--step` để yêu cầu Server cộng dồn số liệu, giảm số lượng request API.
*   **Theo tháng (Monthly):** Lấy data tổng của từng tháng.
    ```bash
    uv run main.py --start 2025-01-01 --end 2025-03-31 --step month
    ```
*   **Tổng hợp (Total):** Lấy 1 cục tổng duy nhất từ ngày A đến ngày B.
    ```bash
    uv run main.py --start 2025-01-01 --end 2025-12-31 --step total
    ```

### 3. Chế độ an toàn (Safety First)
*   **Dry Run (`--dry-run`):** Kiểm tra logic phân chia ngày tháng mà **KHÔNG** gửi request thật, **KHÔNG** ghi file.
    ```bash
    uv run main.py --start 2025-01-01 --end 2025-12-31 --step month --dry-run
    ```
*   **Debug (`--debug`):** In chi tiết URL, Params và Response (nếu có lỗi) để kiểm tra.
    ```bash
    uv run main.py --start ... --end ... --debug
    ```

### 4. Chế độ Offline (`--mode offline`)
Bỏ qua bước cào data, chỉ thực hiện gộp và xử lý các file Parquet đã có sẵn trong máy.
```bash
uv run main.py --mode offline
```

---

## 🧠 Kiến trúc hệ thống (Architecture & Workflow)

Luồng đi của dữ liệu (Pipeline) được chia thành 3 giai đoạn chính:

### Phase 1: Authentication (Người gác cổng)
*   **AutoLogin (Playwright):** Tool khởi động một trình duyệt ẩn (Headless Chromium), tự động điền User/Pass từ `.env`, và trích xuất JWT Token từ Local Storage/Cookies.
*   **Stealth Mode:** Trình duyệt được cấu hình để ẩn danh tính "Robot" tránh bị chặn.
*   **Fallback:** Nếu Login thất bại, tool sẽ chờ người dùng copy Token thủ công vào Clipboard (Windows only).

### Phase 2: Harvesting (Người thu hoạch)
*   **Input:** Range ngày (Start -> End) và Độ mịn (Step).
*   **Logic:**
    *   Chia nhỏ khoảng thời gian thành các "Chunks" (Ngày, Tháng, hoặc Năm).
    *   Gửi Request tới API Dashboard.
*   **Bronze Layer (`raw_data/`):** Lưu phản hồi gốc dưới dạng `.xlsx` (Excel) làm bằng chứng đối chiếu.
*   **Silver Layer (`silver_data/`):** Ngay lập tức convert Excel sang **Parquet** (nén ZSTD).
    *   *Tại sao?* Parquet nhẹ hơn Excel 10 lần, đọc nhanh hơn 50 lần, giữ nguyên định dạng kiểu dữ liệu.

### Phase 3: Processing (Bộ xử lý trung tâm)
*   **Polars Engine:** Sử dụng thư viện `polars` để quét toàn bộ file trong thư mục Silver.
*   **Cleaning:** Làm sạch các cột tiền tệ (xóa `$`, `,`, `%`), ép kiểu số.
*   **Deduplication:** Loại bỏ trùng lặp dựa trên `SKU` và `Report_Date` (ưu tiên dữ liệu mới nhất).
*   **Output:** Tạo file `Master_PPC_Data.parquet` (và `.csv` backup) sẵn sàng để n8n hoặc BI Tool tiêu thụ.

---

## 📂 Cấu trúc thư mục

```
scrape_tool/
├── .env                # Chứa Credential (User/Pass) - KHÔNG COMMIT FILE NÀY
├── config.py           # URL, Selectors, cấu hình mặc định
├── main.py             # Entry point
├── scrape_bot.py       # Logic chính (Core)
├── raw_data/           # Chứa file Excel gốc (.xlsx)
├── silver_data/        # Chứa file Parquet đã convert (.parquet)
└── exports/            # Chứa các báo cáo đầu ra (.csv, .parquet)
```

## ⚠️ Lưu ý quan trọng
*   Đừng spam request quá nhanh (Logic code đã có `sleep(1)` giữa các request).
*   Luôn kiểm tra `--dry-run` trước khi chạy range ngày lớn (ví dụ cả năm).