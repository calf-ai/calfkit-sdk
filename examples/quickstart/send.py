import asyncio

from calfkit.client import Client


async def main():
    # ``async with`` shuts the client down cleanly on exit (producer/consumer
    # teardown). Delivery does not depend on it: ``send`` awaits the broker's
    # ack before returning, so the record is already on the topic.
    async with Client.connect("localhost:9092") as client:  # Connect to Kafka broker
        # Dispatch without awaiting: send() returns a Dispatch (a fire token) as soon as the broker
        # acks the record — it registers no per-run handle, so you can't retrieve the result by id.
        dispatch = await client.agent("weather_agent").send("What's the weather in Tokyo?")
        print(f"Dispatched — correlation_id={dispatch.correlation_id}")

    # The reply still lands on this client's inbox. To observe a send()'s result, keep a firehose
    # running (`async with client.events() as stream:` — subscribe BEFORE you send), or tap the agent's
    # publish_topic broadcast (run weather_sink.py, a @consumer on weather_agent.output). For a result
    # you must not miss, use start()/execute() (which hold a handle) instead of send().


if __name__ == "__main__":
    asyncio.run(main())
