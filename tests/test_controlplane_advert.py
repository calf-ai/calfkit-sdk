"""Unit tests for the @advertises decorator + AdvertRegistryMixin (spec §6, plan §3.2)."""

from datetime import datetime, timezone

import pytest

from calfkit._registry import RegistryMixin, handler
from calfkit.controlplane import ControlPlaneRecord, ControlPlaneStamp, advertises
from calfkit.controlplane.advert import AdvertInfo, AdvertRegistryMixin, advert_info
from calfkit.exceptions import RegistryConfigError
from calfkit.nodes import BaseNodeDef


class _Rec(ControlPlaneRecord):
    schema_version: int = 1
    content: str


class _Rec2(ControlPlaneRecord):
    schema_version: int = 1
    other: str


def _stamp() -> ControlPlaneStamp:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return ControlPlaneStamp(started_at=ts, last_heartbeat_at=ts, heartbeat_interval=30.0)


# -- decorator ---------------------------------------------------------------


def test_decorator_returns_method_unchanged_and_stamps_marker() -> None:
    class T(AdvertRegistryMixin):
        @advertises(topic="t", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="x")

    # the method is still directly callable (marker-not-wrapper)
    info = advert_info(T._r)
    assert isinstance(info, AdvertInfo)
    assert info.topic == "t"
    assert info.record is _Rec
    assert T()._r(_stamp()).content == "x"


def test_advertises_rejects_empty_topic() -> None:
    with pytest.raises(ValueError, match="topic"):
        advertises(topic="", record=_Rec)


def test_advertises_rejects_non_record_type() -> None:
    class NotARecord:
        pass

    with pytest.raises(ValueError, match="ControlPlaneRecord"):
        advertises(topic="t", record=NotARecord)  # type: ignore[type-var]


# -- collection --------------------------------------------------------------


def test_collection_per_type() -> None:
    class T(AdvertRegistryMixin):
        @advertises(topic="calf.x", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="x")

    assert set(T._adverts) == {"calf.x"}
    assert T._adverts["calf.x"].record is _Rec
    # control_plane_adverts resolves bound factories
    factories = T().control_plane_adverts()
    assert set(factories) == {"calf.x"}
    assert factories["calf.x"](_stamp()).content == "x"


def test_duplicate_topic_raises_at_class_definition() -> None:
    with pytest.raises(RegistryConfigError, match="unique per class"):

        class T(AdvertRegistryMixin):
            @advertises(topic="dup", record=_Rec)
            def _a(self, identity: ControlPlaneStamp) -> _Rec:
                return _Rec(**identity.model_dump(), content="a")

            @advertises(topic="dup", record=_Rec)
            def _b(self, identity: ControlPlaneStamp) -> _Rec:
                return _Rec(**identity.model_dump(), content="b")


def test_no_adverts_is_empty() -> None:
    class T(AdvertRegistryMixin):
        pass

    assert T._adverts == {}
    assert T().control_plane_adverts() == {}


def test_mro_override_redecorated_most_derived_wins() -> None:
    class Base(AdvertRegistryMixin):
        @advertises(topic="t", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="base")

    class Derived(Base):
        @advertises(topic="t", record=_Rec2)  # re-decorated: new record type wins
        def _r(self, identity: ControlPlaneStamp) -> _Rec2:  # type: ignore[override]
            return _Rec2(**identity.model_dump(), other="derived")

    assert Derived._adverts["t"].record is _Rec2
    assert Derived().control_plane_adverts()["t"](_stamp()).other == "derived"


def test_mro_override_without_redecorate_resolves_to_override() -> None:
    class Base(AdvertRegistryMixin):
        @advertises(topic="t", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="base")

    class Derived(Base):
        def _r(self, identity: ControlPlaneStamp) -> _Rec:  # override, NOT re-decorated
            return _Rec(**identity.model_dump(), content="derived")

    # advert still registered (inherited), bound factory is the override
    assert set(Derived._adverts) == {"t"}
    assert Derived().control_plane_adverts()["t"](_stamp()).content == "derived"


# -- coexistence with @handler ----------------------------------------------


def test_coexists_with_handler_registry() -> None:
    class T(RegistryMixin, AdvertRegistryMixin):
        @handler("*")
        async def run(self, ctx: object) -> None: ...

        @advertises(topic="t", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="x")

    assert "*" in T._handlers
    assert "t" in T._adverts


def test_basenodedef_collects_adverts_and_keeps_handlers() -> None:
    class _Node(BaseNodeDef):
        @advertises(topic="calf.node", record=_Rec)
        def _r(self, identity: ControlPlaneStamp) -> _Rec:
            return _Rec(**identity.model_dump(), content="x")

    # adverts collected via the mixin attached to BaseNodeDef
    assert set(_Node._adverts) == {"calf.node"}
    # the inherited run handler ('*') is still present (registries are independent)
    assert "*" in _Node._handlers


def test_basenodedef_without_adverts_is_empty() -> None:
    assert BaseNodeDef._adverts == {}


def test_advert_info_rejects_empty_topic() -> None:
    with pytest.raises(ValueError, match="topic"):
        AdvertInfo(topic="", record=_Rec, name="x")


def test_advert_info_rejects_empty_name() -> None:
    with pytest.raises(ValueError, match="name"):
        AdvertInfo(topic="t", record=_Rec, name="")


def test_advert_info_rejects_non_record_type() -> None:
    # AdvertInfo is public/constructible directly; self-validate the record type
    # (parity with the @advertises decorator and HandlerInfo's self-validation).
    class NotARecord:
        pass

    with pytest.raises(ValueError, match="ControlPlaneRecord"):
        AdvertInfo(topic="t", record=NotARecord, name="x")  # type: ignore[arg-type]
