import unittest
import sys
import os
import shutil
import polars as pl
from datetime import datetime
import time

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modern_etl import RawToSilverIngester, ETLLogger

class TestIngestion(unittest.TestCase):
    
    def setUp(self):
        # Setup paths
        self.test_raw_dir = "./test_raw_data"
        self.test_silver_dir = "./test_silver_data"
        
        # Create directories
        for d in [self.test_raw_dir, self.test_silver_dir]:
            if not os.path.exists(d):
                os.makedirs(d)
                
        # Create a dummy CSV file
        self.dummy_csv = os.path.join(self.test_raw_dir, "dummy_report.csv")
        df = pl.DataFrame({
            "SKU": ["A1", "B2"],
            "Revenue": [100, 200]
        })
        df.write_csv(self.dummy_csv)
        
        # Init components
        self.logger = ETLLogger("test_etl.log")
        self.ingester = RawToSilverIngester()
        # Override logger in ingester (we will add this dependency injection in implementation)
        self.ingester.logger = self.logger 

    def tearDown(self):
        # Cleanup
        if os.path.exists(self.test_raw_dir):
            shutil.rmtree(self.test_raw_dir)
        if os.path.exists(self.test_silver_dir):
            shutil.rmtree(self.test_silver_dir)
        if os.path.exists("test_etl.log"):
            os.remove("test_etl.log")

    def test_ingest_happy_path(self):
        """Test standard ingestion flow"""
        metadata = {
            "start_date": "2025-10-01",
            "end_date": "2025-10-02",
            "base_dir": self.test_silver_dir # Inject test dir to override default
        }
        
        # Action
        output_path = self.ingester.ingest_file(self.dummy_csv, metadata)
        
        # Assertions
        self.assertIsNotNone(output_path, "Ingestion returned None (Failed)")
        self.assertTrue(os.path.exists(output_path), "Output parquet file not found")
        self.assertTrue(output_path.endswith(".parquet"), "Output is not .parquet")
        
        # Check folder structure: test_silver_data/2025/10/...
        expected_folder_part = os.path.join("2025", "10")
        self.assertIn(expected_folder_part, output_path)
        
        # Check Data Content
        df_result = pl.read_parquet(output_path)
        
        # Check Ingestion Time Stamping
        self.assertIn("ingestion_time", df_result.columns)
        self.assertIn("Date_Start", df_result.columns)
        
        # Check Data Integrity
        self.assertEqual(df_result["SKU"][0], "A1")

    def test_no_overwrite(self):
        """Ensure subsequent runs create new files, not overwrite"""
        metadata = {
            "start_date": "2025-10-01",
            "end_date": "2025-10-02",
            "base_dir": self.test_silver_dir
        }
        
        path1 = self.ingester.ingest_file(self.dummy_csv, metadata)
        time.sleep(1) # Ensure timestamp differs
        path2 = self.ingester.ingest_file(self.dummy_csv, metadata)
        
        self.assertNotEqual(path1, path2, "Files should have different names due to timestamp")
        self.assertTrue(os.path.exists(path1))
        self.assertTrue(os.path.exists(path2))

if __name__ == '__main__':
    unittest.main()
