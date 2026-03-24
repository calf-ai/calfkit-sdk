import asyncio
import concurrent.futures
from datetime import timedelta

# Import the activity and workflow from our other files
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class SayHello:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, schedule_to_close_timeout=timedelta(seconds=5)
        )


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Run the worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
            client,
            task_queue="my-task-queue",
            workflows=[SayHello],
            activities=[say_hello],
            activity_executor=activity_executor,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
