"""Canned lookup tools for the help-desk experts — deterministic and offline."""

from calfkit import agent_tool


@agent_tool
def pto_balance(employee: str) -> str:
    """Look up an employee's remaining paid-time-off balance, in days."""
    return f"{employee} has 12 paid-time-off days remaining this year."


@agent_tool
def account_status(username: str) -> str:
    """Look up the status of a user's account and access."""
    return f"Account '{username}' is active; VPN enabled, password expires in 14 days."


@agent_tool
def file_reimbursement(amount_usd: float, purpose: str) -> str:
    """File an expense reimbursement and return the confirmation."""
    return f"Filed reimbursement #4821 for ${amount_usd:.2f} ({purpose}); payout in 3-5 business days."
