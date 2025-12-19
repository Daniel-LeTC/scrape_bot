# File chứa các hằng số và cấu hình mặc định
import os

# Cấu hình API
API_BASE_URL = "https://ppc.api.app.tcsys.shop/api/dashboard/market/export"
LOGIN_URL = "https://ppc.app.tcsys.shop/login"
DASHBOARD_URL = "https://ppc.app.tcsys.shop/dashboard"

# Danh sách các fields cần lấy từ API
DEFAULT_FIELDS = (
    "fbaStock,price,productType,phase,mainNiche,unitSold,revenue,tacos,"
    "crActual,crAvg,cr,cpcActual,cpcAvg,cpc,orgActual,orgAvg,org,"
    "pricePlan,refund,adsSpend,priorityScore,targeting,listingScore,hint"
)

# Thư mục lưu trữ
RAW_DATA_DIR = "./raw_data"
SILVER_DATA_DIR = "./silver_data"
OUTPUT_DIR = "./exports"

# Tạo thư mục nếu chưa có
for folder in [RAW_DATA_DIR, SILVER_DATA_DIR, OUTPUT_DIR]:
    if not os.path.exists(folder):
        os.makedirs(folder)


# Header mặc định cho Request
def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
    }
