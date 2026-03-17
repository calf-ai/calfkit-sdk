from uuid_utils import uuid7


def generate_payload_id() -> str:
    """Generate a unique, time sortable payload id"""
    return f"payload_{uuid7().hex}"
