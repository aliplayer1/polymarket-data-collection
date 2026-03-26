import asyncio
import pandas as pd
from polymarket_pipeline.pipeline import PolymarketDataPipeline
from polymarket_pipeline.settings import PipelineRunOptions

async def run():
    pipeline = PolymarketDataPipeline()
    options = PipelineRunOptions.from_values(websocket_only=True, cryptos=["ELON-TWEETS"])
    active_markets = pipeline._collect_active_markets(options, None)
    
    print(f"Streaming for {len(active_markets)} ELON-TWEETS markets for 15 seconds...")
    
    # Run the websocket stream for a limited time to capture ticks
    ws_task = asyncio.create_task(pipeline.run_websocket(active_markets))
    
    await asyncio.sleep(15)
    ws_task.cancel()
    
    try:
        await ws_task
    except asyncio.CancelledError:
        pass
        
    print("\nLooking for collected ticks in data-culture/ticks...")
    culture_dir = pipeline.paths.data_dir.parent / "data-culture"
    import pyarrow.parquet as pq
    import os
    
    if os.path.exists(culture_dir / "ticks"):
        print("Ticks directory exists. Reading parquet files...")
        dataset = pq.ParquetDataset(culture_dir / "ticks")
        df = dataset.read().to_pandas()
        print(f"Collected {len(df)} tick records!")
        if not df.empty:
            print("\nSample Tick:")
            for col, val in df.iloc[0].items():
                print(f"  {col}: {val}")
    else:
        print("No ticks directory found. (Perhaps no trades occurred in those 15 seconds)")

if __name__ == "__main__":
    asyncio.run(run())
