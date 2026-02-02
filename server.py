from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import math
from typing import Optional

app = FastAPI()

# Allow CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Timeframe mapping to yfinance intervals
# Must match TimeframeKey in types.ts exactly
TIMEFRAME_MAP = {
    "Y": "3mo",   # Year approx by 3-month bars
    "Q": "3mo",   # Quarterly
    "M": "1mo",   # Monthly
    "W": "1wk",   # Weekly
    "D": "1d",    # Daily
    "1H": "1h",   # 1 Hour (standard yf interval)
    "30M": "30m", # 30 Minute
    "15M": "15m", # 15 Minute
}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/candles")
async def get_candles(
    ticker: str, 
    timeframe: str, 
    limit: Optional[int] = Query(300) # Prevents 400 error when frontend sends limit
):
    # Standardize and validate timeframe
    tf_key = timeframe.strip().upper()
    interval = TIMEFRAME_MAP.get(tf_key)
    
    if not interval:
        supported = ", ".join(TIMEFRAME_MAP.keys())
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid timeframe: {timeframe}. Supported: {supported}"
        )
    
    # Drummond Geometry requires enough bars to calculate averages (at least 10+)
    # yf constraints: 1h interval only allows up to 730 days of data.
    if tf_key in ["Y", "Q", "M"]:
        period = "max"
    elif tf_key == "W":
        period = "10y"
    elif tf_key == "D":
        period = "2y"
    elif tf_key == "1H":
        period = "730d" # Max for 1h/60m
    else:
        period = "30d"

    try:
        print(f"Request: {ticker} @ {tf_key} (Interval: {interval}, Period: {period})")
        
        # Fetching data with auto_adjust=True to fix warnings
        df = yf.download(
            ticker, 
            period=period, 
            interval=interval, 
            progress=False, 
            auto_adjust=True
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for {ticker}")
        
        # Flatten MultiIndex columns (ticker-level)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.dropna()

        candles = []
        for timestamp, row in df.iterrows():
            candles.append({
                "time": int(timestamp.timestamp() * 1000),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close']),
                "volume": int(row['Volume']) if 'Volume' in row and not math.isnan(row['Volume']) else 0
            })
            
        # Ensure we return at least a few bars for Drummond math
        if len(candles) < 10:
             raise HTTPException(status_code=422, detail=f"Insufficient historical data for {tf_key}")

        return candles[-limit:] if len(candles) > limit else candles
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data Source Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
