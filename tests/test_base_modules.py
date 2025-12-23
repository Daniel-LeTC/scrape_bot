import unittest
import sys
import os
import shutil
from datetime import datetime
import logging

# Add parent directory to path to import modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modern_etl import PartitionManager, ETLLogger

class TestBaseModules(unittest.TestCase):
    
    def setUp(self):
        # Setup temporary test directory
        self.test_dir = "./test_silver_data"
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)

    def tearDown(self):
        # Cleanup after tests
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_partition_creation(self):
        """Test if PartitionManager creates correct folder structure"""
        date_obj = datetime(2025, 10, 15) # Oct 15, 2025
        
        expected_path = os.path.join(self.test_dir, "2025", "10")
        
        # Call the function (Need to implement it first for this to pass)
        # Note: We are testing the logic we ARE ABOUT TO write.
        real_path = PartitionManager.ensure_partition_exists(self.test_dir, date_obj)
        
        self.assertEqual(real_path, expected_path)
        self.assertTrue(os.path.exists(real_path))
        self.assertTrue(os.path.isdir(real_path))

    def test_logger_init(self):
        """Test if Logger initializes without error"""
        try:
            logger = ETLLogger("test_log.log")
            self.assertIsInstance(logger.logger, logging.Logger)
        except Exception as e:
            self.fail(f"Logger init failed with error: {e}")

if __name__ == '__main__':
    unittest.main()
