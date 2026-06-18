import pytest

from services.ingestion.replay import (
    DeterministicReplayFeed,
    ReplayDisconnectError,
    build_default_replay_events,
    build_replay_feed_factory,
)


@pytest.mark.asyncio
async def test_replay_feed_yields_fixed_events() -> None:
    seen = []

    async for event in DeterministicReplayFeed(build_default_replay_events()):
        seen.append(event)

    assert seen == build_default_replay_events()


@pytest.mark.asyncio
async def test_replay_feed_can_simulate_disconnect() -> None:
    iterator = DeterministicReplayFeed(
        build_default_replay_events(),
        disconnect_at=1,
    )

    with pytest.raises(ReplayDisconnectError):
        async for _event in iterator:
            pass


@pytest.mark.asyncio
async def test_replay_feed_factory_disconnects_once_across_instances() -> None:
    factory = build_replay_feed_factory(build_default_replay_events(), disconnect_at=0)

    with pytest.raises(ReplayDisconnectError):
        async for _event in factory():
            pass

    seen = []
    async for event in factory():
        seen.append(event)

    assert seen == build_default_replay_events()


@pytest.mark.asyncio
async def test_replay_feed_factory_preserves_empty_event_list() -> None:
    seen = []

    async for event in build_replay_feed_factory([])():
        seen.append(event)

    assert seen == []
