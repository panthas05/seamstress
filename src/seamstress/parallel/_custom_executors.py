import collections
import threading
import typing


class PropagatingThread(threading.Thread):
    """
    Propagates any exceptions raised in the thread back to the (main) thread that
    spawned it.
    """

    _exception_raised_by_target: BaseException | None
    _exception_lock: threading.Lock

    def __init__(
        self,
        group: None = None,
        target: collections.abc.Callable[..., object] | None = None,
        name: str | None = None,
        args: collections.abc.Iterable[typing.Any] = (),
        kwargs: collections.abc.Mapping[str, typing.Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        self._exception_lock = threading.Lock()
        self._exception_raised_by_target = None

        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )

    def run(self) -> None:
        try:
            return super().run()
        except BaseException as e:
            with self._exception_lock:
                self._exception_raised_by_target = e

    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)

        with self._exception_lock:
            if self._exception_raised_by_target:
                raise self._exception_raised_by_target
