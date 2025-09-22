#!/usr/bin/env python3
"""
TracktionAI ETL Pipeline Runner
==============================
Simple script to run the complete ETL pipeline for processing 11GB dataset
"""

import sys
import os
import logging
from pathlib import Path

# Add the ETL directory to Python path
etl_dir = Path(__file__).parent
sys.path.append(str(etl_dir))

from process_large_dataset import TracktionAIETL

def setup_logging():
    """Setup comprehensive logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl_pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 TracktionAI ETL Pipeline Runner Starting...")
    return logger

def main():
    """Run the complete ETL pipeline"""
    logger = setup_logging()
    
    try:
        # Verify we're in the correct directory
        expected_files = [
            'output/listen_events',
            'output/auth_events', 
            'output/page_view_events',
            'output/status_change_events'
        ]
        
        missing_files = []
        for file_path in expected_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
            logger.error("   Please ensure you're running from the project root directory")
            logger.error("   and that all 11GB data files are in the output/ directory")
            return 1
        
        logger.info("✅ All required data files found")
        logger.info("📊 Starting ETL pipeline processing...")
        logger.info("⏱️  Estimated processing time: 15-30 minutes")
        logger.info("💾 This will process ~23M records and generate comprehensive analytics")
        
        # Initialize and run ETL pipeline
        etl = TracktionAIETL(chunk_size=25000)  # Conservative chunk size
        etl.run_etl_pipeline()
        
        logger.info("🎉 ETL Pipeline completed successfully!")
        logger.info("📁 Generated files:")
        logger.info("   - aggregated_music_data.json (main analytics)")
        logger.info("   - Multiple CSV files in aggregated_data/ directory")
        logger.info("   - Updated dashboard data sources")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Pipeline interrupted by user")
        return 1
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed with error: {e}")
        logger.error("   Check etl_pipeline.log for detailed error information")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
