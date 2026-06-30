"""Canned status tools for the launch-readiness experts — deterministic and offline."""

from calfkit import agent_tool


@agent_tool
def build_status() -> str:
    """Report the current build and test status."""
    return "Build green on main; 2 flaky integration tests (non-blocking); release branch cut."


@agent_tool
def security_scan() -> str:
    """Report the latest security scan results."""
    return "No critical or high findings; 1 medium (rate-limit hardening) scheduled for next sprint."


@agent_tool
def compliance_status() -> str:
    """Report the legal and compliance review status."""
    return "Updated Terms of Service approved; data-processing addendum signed; cleared for launch."
