import asyncio
from polymarket_pipeline.pipeline import PolymarketDataPipeline
from polymarket_pipeline.settings import PipelineRunOptions

async def run():
    pipeline = PolymarketDataPipeline()
    options = PipelineRunOptions.from_values(websocket_only=True, cryptos=["ELON-TWEETS"])
    active_markets = pipeline._collect_active_markets(options, None)
    
    print(f"Found {len(active_markets)} active ELON-TWEETS markets.")
    
    # Just print the first 3 to show exact data records
    for m in active_markets[:3]:
        print(f"\nMarket: {m.question}")
        print(f"Type: {m.market_type} | Timeframe: {m.timeframe} | Crypto: {m.crypto}")
        print(f"Tokens: {m.tokens}")

if __name__ == "__main__":
    asyncio.run(run())
