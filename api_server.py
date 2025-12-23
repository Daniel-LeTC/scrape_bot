from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import sys
import os

# Add parent directory to path to import local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modern_etl import ETLLogger

# Initialize App
app = FastAPI(title="PPC Data Ingestion API", version="1.0.0")
logger = ETLLogger("api_server.log")

# --- Pydantic Models ---
class ScrapeRequest(BaseModel):
    start_date: str
    end_date: str
    step: Optional[str] = "day" # day, month, year, total

# --- Endpoints ---

@app.get("/health")
def health_check():
    """Simple health check endpoint"""
    return {"status": "ok", "service": "ppc-ingest-api"}

@app.post("/trigger/scrape")
def trigger_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Triggers the scraper bot to run in background.
    Recommended for both First Run (Backfill) and Daily Schedule.
    """
    # TODO: Import PPCHarvester logic here (lazy import to avoid circular dependency issues if any)
    # from scrape_bot import PPCHarvester
    
    # TODO: Define the background task function wrapper
    # def run_scraper_task(start, end, step):
    #     bot = PPCHarvester(...)
    #     bot.fetch_data(...)
    
    # TODO: Add task to background_tasks
    # background_tasks.add_task(run_scraper_task, request.start_date, request.end_date, request.step)
    
    logger.log_success("API", "Trigger", f"Received scrape request: {request}")
    return {"status": "accepted", "message": "Scrape job started in background", "params": request}

# --- TODO: FUTURE EXPANSION ---
# Endpoint: POST /ingest/memory
# Purpose: Direct JSON ingestion from n8n or external Webhooks.
# Implementation Plan:
# 1. Create Pydantic model for data payload (List[Dict]).
# 2. Use RawToSilverIngester.ingest_memory_data() (to be implemented).
# 3. Useful when n8n acts as the fetcher instead of Python.
if __name__ == "__main__":
    import uvicorn
    # Run with: uv run uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run(app, host="0.0.0.0", port=8000)
