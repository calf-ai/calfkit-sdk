import asyncio

from temporalio.client import Client

# Import the workflow from the previous code
from workflows import SayHello


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute a workflow
    result = await client.execute_workflow(
        SayHello.run, "my name", id="my-workflow-id", task_queue="my-task-queue"
    )

    print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
