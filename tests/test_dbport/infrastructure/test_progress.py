"""Tests for infrastructure.progress — ProgressCallback protocol and contextvar."""

from __future__ import annotations

from dbport.infrastructure.progress import ProgressCallback, progress_callback


class _SimpleProgress:
    """Minimal implementation for protocol conformance testing."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []

    def started(self, description: str, total: int | None = None) -> None:
        self.events.append(("started", description, str(total)))

    def update(self, advance: int) -> None:
        self.events.append(("update", str(advance)))

    def log(self, message: str) -> None:
        self.events.append(("log", message))

    def finished(self, message: str | None = None) -> None:
        self.events.append(("finished", str(message)))


class TestProgressCallback:
    def test_default_is_none(self):
        assert progress_callback.get(None) is None

    def test_set_and_reset(self):
        impl = _SimpleProgress()
        token = progress_callback.set(impl)
        assert progress_callback.get(None) is impl
        progress_callback.reset(token)
        assert progress_callback.get(None) is None

    def test_simple_impl_satisfies_protocol(self):
        impl = _SimpleProgress()
        assert isinstance(impl, ProgressCallback)

    def test_lifecycle(self):
        impl = _SimpleProgress()
        token = progress_callback.set(impl)
        try:
            cb = progress_callback.get(None)
            assert cb is not None
            cb.started("Loading test.table", total=1000)
            cb.update(500)
            cb.update(500)
            cb.finished("Done")
            assert len(impl.events) == 4
            assert impl.events[0] == ("started", "Loading test.table", "1000")
            assert impl.events[1] == ("update", "500")
            assert impl.events[3] == ("finished", "Done")
        finally:
            progress_callback.reset(token)

    def test_indeterminate_mode(self):
        impl = _SimpleProgress()
        impl.started("Spinner task", total=None)
        assert impl.events[0] == ("started", "Spinner task", "None")

    def test_log_method(self):
        impl = _SimpleProgress()
        impl.log("Switching to streaming Arrow fallback")
        assert impl.events == [("log", "Switching to streaming Arrow fallback")]
