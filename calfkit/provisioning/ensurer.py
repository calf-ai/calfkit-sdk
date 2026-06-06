"""Startup topic provisioning orchestrator (issue #180).

:class:`StartupTopicEnsurer` collects the topics a process needs (the client's
reply topic; a worker's node topics) and, **when provisioning is enabled**,
creates them at broker start — before any subscriber begins consuming — reusing
FastStream's broker-managed admin client. It is wired as a generic pre-start
hook on the client's broker, so every start path (bare ``broker.start()``,
``Worker.run()``/``start()``/``async with``, ``calfkit run``) is covered.

When provisioning is **disabled** (the default), ``run`` is a pure no-op: the
admin client is never touched and the broker's own / cluster's topic-creation
behaviour is unchanged.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from calfkit.exceptions import MissingTopicsError
from calfkit.provisioning.config import ProvisioningConfig
from calfkit.provisioning.provisioner import provision_topics

logger = logging.getLogger(__name__)


def _admin_client_or_none(broker: Any) -> Any | None:
    """Return FastStream's broker-managed admin client, or ``None`` when it is
    unavailable (a ``consumer_only`` broker, or one that is not yet connected).

    A single guarded accessor for the one FastStream internal we depend on, so a
    future rename surfaces in exactly one place. ``IncorrectState`` is the
    framework's "admin client is not initialized" signal — treated as "no admin
    available, skip provisioning" rather than an error.
    """
    from faststream.exceptions import IncorrectState

    try:
        return broker.config.broker_config.admin_client
    except IncorrectState:
        return None


class StartupTopicEnsurer:
    """Holds the provisioning policy + a declared-topic registry, and creates the
    declared topics at broker start (when enabled) via the broker's admin client.

    Topic *computation* lives with the caller (the client declares its reply
    topic; the worker declares ``topics_for_nodes``); this class owns only the
    *policy* and the *execution*, keeping the broker a generic transport.
    """

    def __init__(self, *, config: ProvisioningConfig) -> None:
        self._config = config
        # topic -> is_framework. Framework inboxes (reply / *.private.return)
        # never receive user ``topic_configs``.
        self._declared: dict[str, bool] = {}

    def declare(self, topics: Iterable[str], *, framework: bool = False) -> None:
        """Register ``topics`` to be ensured at start. ``framework=True`` once is
        sticky (a topic declared as both data and framework stays framework)."""
        for topic in topics:
            self._declared[topic] = self._declared.get(topic, False) or framework

    async def run(self, broker: Any) -> None:
        """Provision the declared topics if provisioning is enabled.

        No-op (and the admin client is never touched) when disabled or when
        nothing was declared. Skips with a log when no admin client is available
        (``consumer_only`` deployments). Raises :class:`MissingTopicsError` when
        provisioning was requested but a declared topic could not be created.
        """
        if not self._config.enabled or not self._declared:
            return
        admin = _admin_client_or_none(broker)
        if admin is None:
            logger.info("topic provisioning enabled but no admin client available (consumer_only?); skipping")
            return

        framework = {t for t, is_fw in self._declared.items() if is_fw}
        report = await provision_topics(
            admin,
            list(self._declared),
            framework_topics=framework,
            config=self._config,
        )
        logger.info(
            "provisioned topics: %d created, %d existing, %d unauthorized",
            len(report.created),
            len(report.existing),
            len(report.unauthorized),
        )

        existing = set(report.created) | set(report.existing)
        uncreated = [t for t in self._declared if t not in existing]
        if uncreated:
            raise MissingTopicsError(uncreated)
