import os
import logging
import datetime
import traceback
from datetime import datetime
import fastexcel
import polars as pl

# Config constants (Temporary placement, ideally should come from config.py)
SILVER_DATA_DIR = "./silver_data"
RAW_DATA_DIR = "./raw_data"

class ETLLogger:
    """
    Centralized logging specifically for ETL jobs.
    Focus: Traceability (Action, Source, Error).
    """
    def __init__(self, log_file="etl_process.log"):
        self.logger = logging.getLogger("ETL_Worker")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            # File Handler
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.INFO)
            
            # Console Handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            # Formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def log_success(self, action, source, message):
        self.logger.info(f"ACTION: {action} | SOURCE: {source} | MSG: {message}")

    def log_error(self, action, source, error_obj):
        error_msg = str(error_obj)
        stack_trace = traceback.format_exc()
        self.logger.error(f"ACTION: {action} | SOURCE: {source} | ERROR: {error_msg}\nTRACE: {stack_trace}")


class PartitionManager:
    """
    Responsible for storage layout.
    Philosophy: Partition by Time (Year/Month).
    """
    @staticmethod
    def ensure_partition_exists(base_dir, date_obj):
        """
        Input: date_obj (datetime)
        Output: Path string (e.g., './silver_data/2025/10')
        Action: Creates directory if not exists.
        """
        year = str(date_obj.year)
        month = f"{date_obj.month:02d}"
        target_path = os.path.join(base_dir, year, month)
        
        if not os.path.exists(target_path):
            os.makedirs(target_path, exist_ok=True)
            
        return target_path


class RawToSilverIngester:
    """
    The 'Stamping' Worker.
    Responsibility: Read Raw -> Add Metadata (Ingestion Time) -> Write Parquet.
    Constraint: Never overwrite, always append/create new file.
    """
    def __init__(self, logger=None):
        self.logger = logger or ETLLogger()

    def ingest_file(self, raw_file_path, metadata_dict):
        """
        Args:
            raw_file_path (str): Path to .xlsx or .csv
            metadata_dict (dict): Context data (start_date, end_date, sku_prefix...)
                - Must contain: 'end_date' (YYYY-MM-DD) for partitioning.
                - Optional: 'base_dir' to override default storage.
        Returns:
            str: Path to the generated parquet file, or None if failed.
        """
        try:
            if not os.path.exists(raw_file_path):
                self.logger.log_error("Ingest", raw_file_path, FileNotFoundError("File not found"))
                return None

            # --- STEP 1: READ DATA ---
            # Determine logic based on extension
            file_ext = os.path.splitext(raw_file_path)[1].lower()
            
            if file_ext == '.xlsx':
                # Use fastexcel for reading Excel
                excel_reader = fastexcel.read_excel(raw_file_path)
                arrow_table = excel_reader.load_sheet(0).to_arrow()
                df = pl.from_arrow(arrow_table)
            elif file_ext == '.csv':
                df = pl.read_csv(raw_file_path)
            else:
                self.logger.log_error("Ingest", raw_file_path, ValueError(f"Unsupported format: {file_ext}"))
                return None

            if df.height == 0:
                self.logger.log_success("Ingest", raw_file_path, "Skipped empty file.")
                return None

            # Delegate to shared processing logic
            return self._process_and_write(df, metadata_dict, source_name=raw_file_path)

        except Exception as e:
            self.logger.log_error("Ingest", raw_file_path, e)
            return None

    def _process_and_write(self, df, metadata_dict, source_name="unknown"):
        """
        Internal method: Stamps -> Partitions -> Writes.
        Used by both file ingestion and memory ingestion.
        """
        try:
            # --- STEP 2: STAMPING (METADATA INJECTION) ---
            # 1. Ingestion Time (The Source of Truth)
            ingestion_ts = datetime.now().isoformat()
            df = df.with_columns(pl.lit(ingestion_ts).alias("ingestion_time"))

            # 2. Business Metadata (Start Date, End Date...)
            if "start_date" in metadata_dict:
                 df = df.with_columns(pl.lit(metadata_dict["start_date"]).alias("Date_Start"))
            if "end_date" in metadata_dict:
                 df = df.with_columns(pl.lit(metadata_dict["end_date"]).alias("Date_End"))
                 df = df.with_columns(pl.lit(metadata_dict["end_date"]).alias("Report_Date"))

            # --- STEP 3: PARTITIONING ---
            end_date_str = metadata_dict.get("end_date", datetime.now().strftime("%Y-%m-%d"))
            try:
                date_obj = datetime.strptime(end_date_str, "%Y-%m-%d")
            except ValueError:
                date_obj = datetime.now()

            base_dir = metadata_dict.get("base_dir", SILVER_DATA_DIR)
            target_dir = PartitionManager.ensure_partition_exists(base_dir, date_obj)

            # --- STEP 4: NAMING STRATEGY ---
            start = metadata_dict.get("start_date", "unknown")
            end = metadata_dict.get("end_date", "unknown")
            safe_ts = datetime.now().strftime("%Y%m%d%H%M%S%f")
            
            filename = f"ppc_{start}_{end}_ingest_{safe_ts}.parquet"
            output_path = os.path.join(target_dir, filename)

            # --- STEP 5: WRITE ---
            df.write_parquet(output_path, compression="zstd")
            
            self.logger.log_success("Ingest", source_name, f"Successfully stamped and saved to {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.log_error("Process & Write", source_name, e)
            return None

    def ingest_from_folder(self, folder_path, pattern="*", metadata_dict=None):
        """
        [SKELETON] Scenario: Backfill from Local Dump.
        Iterates through a folder, finds matching files, and ingests them one by one.
        """
        # TODO: Use glob to find files matching pattern
        # TODO: Loop through files
        # TODO: For each file, try to extract date from filename if not provided in metadata
        # TODO: Call self.ingest_file()
        pass

    def ingest_memory_data(self, data_list, metadata_dict):
        """
        [IMPLEMENTED] Scenario: n8n/Webhook/DB Payload.
        Ingests a list of dictionaries directly from memory.
        """
        try:
            if not data_list:
                self.logger.log_success("Ingest Memory", "Memory", "Skipped empty data list.")
                return None
            
            # Convert list of dicts to Polars DataFrame
            df = pl.from_dicts(data_list)
            
            # Use shared logic
            return self._process_and_write(df, metadata_dict, source_name="memory_payload")
            
        except Exception as e:
            self.logger.log_error("Ingest Memory", "Memory", e)
            return None

    def _standardize_schema(self, df):
        """
        [SKELETON] Scenario: Schema Drift.
        Ensures the DataFrame has a consistent set of columns before writing.
        """
        # TODO: Load a 'Master Schema' config (optional)
        # TODO: Add missing columns with null values
        # TODO: Cast columns to correct types (String -> Float)
        # TODO: Return standardized DF
        return df


# TODO: Add a specific test/runner function here to verify this module independently.
if __name__ == "__main__":
    print("ETL Module Skeleton Loaded.")
