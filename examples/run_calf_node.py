
from dotenv import load_dotenv
from calf.calf_atomic_node import CalfAtomicNode, on, post_to
from calf.runtime import CalfRuntime
from faststream.kafka import TestKafkaBroker

from calf.test_runtime import InMemoryCalfRuntime

load_dotenv()

CalfRuntime.initialize()

class TestCalfNode(CalfAtomicNode):
    
    async def start_test_listen(self) -> str:
        publisher = post_to("another-topic")
        test_msg = "LeStartTest"
        print(f"Writing to test_topic_1 with msg={test_msg}")
        await publisher.publish("Hi!")
        return test_msg

    @on("test_topic_1")
    def test_listen(self, msg: str):
        print(f"Reacting to test_topic_1, msg recieved: {msg}")
    
    @post_to("test_topic_2")
    def start_test_flow(self) -> str:
        test_msg = "LeStartTest"
        print(f"Writing to test_topic_2 with msg={test_msg}")
        return test_msg
    
    @on("test_topic_2")
    @post_to("test_topic_3")
    def test_flow(self, msg: str) -> str:
        print("Reacting to test_topic_2")
        result = msg + "processed value"
        print(f"Publishing '{result}' to test_topic_3")
        return result
    
    @on("test_topic_3")
    def test_flow_end(self, msg: str):
        print("Reacting to test_topic_3")
        assert msg.endswith("processed value")
        print("Success")        

test_node = TestCalfNode()
    
app = test_node.runnable

test_node.start_test_listen()
    

print("Done")
    