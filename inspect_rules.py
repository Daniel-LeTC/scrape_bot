import polars as pl
import os

file_path = "../notes/RULE LẤY DỮ LIỆU PPC TOOL (FBA&FBM&KDP).xlsx"

if not os.path.exists(file_path):
    print(f"Error: File not found at {file_path}")
else:
    try:
        # Đọc tất cả các sheet
        xls = pl.read_excel(file_path, sheet_id=0) # Đọc sheet đầu tiên để test
        
        # Dùng fastexcel engine để lấy sheet names (polars read_excel trả về DataFrame của sheet được chỉ định)
        # Cách đơn giản hơn để debug cấu trúc: Dùng pandas để lấy sheet names cho lẹ vì polars read_excel hơi khác
        import pandas as pd
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        print(f"--- FILE STRUCTURE ---")
        print(f"File: {file_path}")
        print(f"Sheets found: {sheet_names}")
        print("-" * 30)

        for sheet in sheet_names:
            print(f"\n>>> READING SHEET: {sheet}")
            try:
                # Đọc full sheet đầu tiên
                df = pd.read_excel(file_path, sheet_name=sheet)
                
                print(f"--- ALL COLUMNS IN '{sheet}' ---")
                print(list(df.columns))
                print("-" * 30)
                
                # Lọc các cột có vẻ chứa logic (Score, Hint, Rule, Status...) hoặc các cột cuối
                # Logic: Lấy các cột có keyword hoặc 10 cột cuối cùng
                keywords = ['Score', 'Hint', 'Rule', 'Status', 'Condition', 'Priority', 'Listing']
                target_cols = [c for c in df.columns if any(k.lower() in str(c).lower() for k in keywords)]
                
                # Nếu không tìm thấy cột nào đặc biệt, lấy 10 cột cuối
                if not target_cols:
                    target_cols = df.columns[-10:]
                
                print(f"\n>>> LOGIC/METRIC COLUMNS SAMPLE (First 10 rows):")
                print(df[target_cols].head(10))
                
                # Chỉ chạy cho sheet đầu tiên rồi break cho đỡ rối
                break
            except Exception as e:
                print(f"Error reading sheet {sheet}: {e}")

    except Exception as e:
        print(f"Critical Error: {e}")
