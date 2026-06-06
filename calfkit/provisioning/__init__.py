"""Opt-in, best-effort Kafka topic provisioning.

**Experimental** (opt-in; off by default). Everything in this package may
change or be removed in a minor release — calfkit is pre-1.0. Feedback welcome.

See :class:`ProvisioningConfig` for the dev-safe / review-for-prod caveats.
"""

from calfkit.provisioning.config import ProvisioningConfig
from calfkit.provisioning.provisioner import (
    ProvisionReport,
    TopicProvisioner,
    TopicProvisioningError,
    topics_for_nodes,
)

__all__ = [
    "ProvisioningConfig",
    "ProvisionReport",
    "TopicProvisioner",
    "TopicProvisioningError",
    "topics_for_nodes",
]
