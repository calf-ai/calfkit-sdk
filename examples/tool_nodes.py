import asyncio

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_tool_node import agent_tool
from calfkit.runners.service import NodesService

# Tool Nodes - Deploys tool workers that can be called by the agent.

# This runs independently and can be scaled separately from other nodes.

# Usage:
#     uv run python examples/real_broker/tool_nodes.py

# Prerequisites:
#     - Kafka broker running at localhost:9092


# Define tools using the @agent_tool decorator
@agent_tool
async def get_weather(location: str) -> str:
    """Get the current weather for a location.

    Args:
        location: The city or location to get weather for (e.g., New York, London, Tokyo)

    Returns:
        Current weather information including temperature, conditions, and humidity
    """

    # Fake wait
    await asyncio.sleep(5.0)

    # Mock weather data for common locations
    mock_weather = {
        "new york": "Currently 72°F, Partly Cloudy. Humidity: 65%. Wind: 10 mph NW.",
        "london": "Currently 58°F, Overcast with light rain. Humidity: 80%. Wind: 15 mph W.",
        "tokyo": "Currently 68°F, Clear skies. Humidity: 50%. Wind: 5 mph E.",
        "paris": "Currently 63°F, Sunny. Humidity: 55%. Wind: 8 mph NE.",
        "sydney": "Currently 75°F, Cloudy. Humidity: 70%. Wind: 12 mph S.",
        "san francisco": "Currently 65°F, Foggy. Humidity: 75%. Wind: 10 mph W.",
    }

    location_lower = location.lower().strip()
    weather = mock_weather.get(location_lower)
    if weather:
        return f"Weather for {location.title()}: {weather}"
    return f"Weather for {location.title()}: Currently 70°F, Partly Cloudy. Humidity: 60%. Wind: 5 mph."  # noqa: E501


@agent_tool
def get_stock_price(ticker: str) -> str:
    """Get the current stock price for a ticker symbol.

    Args:
        ticker: The stock ticker symbol (e.g., AAPL, GOOGL, TSLA)

    Returns:
        Current stock price and daily change information
    """
    # Mock stock data for common tickers
    mock_stocks = {
        "aapl": {"price": 178.52, "change": "+2.34", "change_pct": "+1.33%", "name": "Apple Inc."},
        "googl": {
            "price": 141.80,
            "change": "-0.89",
            "change_pct": "-0.62%",
            "name": "Alphabet Inc.",
        },
        "tsla": {"price": 248.50, "change": "+5.67", "change_pct": "+2.34%", "name": "Tesla Inc."},
        "msft": {
            "price": 378.91,
            "change": "+1.23",
            "change_pct": "+0.33%",
            "name": "Microsoft Corp.",
        },
        "amzn": {
            "price": 178.25,
            "change": "+3.45",
            "change_pct": "+1.97%",
            "name": "Amazon.com Inc.",
        },
        "nvda": {
            "price": 875.30,
            "change": "+12.45",
            "change_pct": "+1.44%",
            "name": "NVIDIA Corp.",
        },
        "meta": {
            "price": 505.60,
            "change": "-2.15",
            "change_pct": "-0.42%",
            "name": "Meta Platforms Inc.",
        },
    }

    ticker_upper = ticker.upper().strip()
    stock = mock_stocks.get(ticker.lower())
    if stock:
        return f"{stock['name']} ({ticker_upper}): ${stock['price']:.2f} | Change: {stock['change']} ({stock['change_pct']})"  # noqa: E501
    return f"Stock {ticker_upper}: $100.00 | No data available for this ticker."


@agent_tool
def get_exchange_rate(from_currency: str, to_currency: str) -> str:
    """Get the exchange rate between two currencies.

    Args:
        from_currency: The source currency code (e.g., USD, EUR, GBP)
        to_currency: The target currency code (e.g., USD, EUR, GBP)

    Returns:
        Exchange rate information between the two currencies
    """
    # Mock exchange rates (base: USD)
    mock_rates = {
        "USD": 1.0,
        "EUR": 0.92,
        "GBP": 0.79,
        "JPY": 149.50,
        "CAD": 1.36,
        "AUD": 1.53,
        "CHF": 0.88,
        "CNY": 7.24,
        "INR": 83.12,
    }

    from_upper = from_currency.upper().strip()
    to_upper = to_currency.upper().strip()

    if from_upper not in mock_rates:
        return f"Error: Unknown currency '{from_upper}'. Supported: {', '.join(mock_rates.keys())}"
    if to_upper not in mock_rates:
        return f"Error: Unknown currency '{to_upper}'. Supported: {', '.join(mock_rates.keys())}"

    if from_upper == to_upper:
        return f"Exchange rate: 1 {from_upper} = 1 {to_upper}"

    # Calculate cross rate: (to_rate / from_rate)
    rate = mock_rates[to_upper] / mock_rates[from_upper]
    return f"Exchange rate: 1 {from_upper} = {rate:.4f} {to_upper}"


async def main():
    print("=" * 50)
    print("Tool Nodes Deployment")
    print("=" * 50)

    # Connect to the real Kafka broker
    print("\nConnecting to Kafka broker at localhost:9092...")
    broker = BrokerClient(bootstrap_servers="localhost:9092")

    # Deploy tool nodes
    print("Registering tool nodes...")
    service = NodesService(broker)

    service.register_node(get_weather, max_workers=2)
    print(f"  - get_weather registered 2 workers subbed to (topic: {get_weather.publish_to_topic})")

    service.register_node(get_stock_price)
    print(f"  - get_stock_price registered subbed to (topic: {get_stock_price.publish_to_topic})")

    service.register_node(get_exchange_rate)
    print(
        f"  - get_exchange_rate registered subbed to (topic: {get_exchange_rate.publish_to_topic})"
    )

    print("\nTool nodes ready. Waiting for requests...")

    # Run the service (blocking)
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTool nodes stopped.")
