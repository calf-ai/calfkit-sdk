from calf.calf_atomic_node import CalfAtomicNode, on, post_to
from calf.providers.base import ModelClient


class Chat(CalfAtomicNode):
    INPUT_TOPIC = "calf_chat_input_topic"
    OUTPUT_TOPIC = "calf_chat_output_topic"
    
    def __init__(self, model_client: ModelClient):
        return
    
    @on(INPUT_TOPIC)
    @post_to(OUTPUT_TOPIC)
    async def invoke(self):
        
        return
    
        
    