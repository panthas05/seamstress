import abc
import types


class ProcessConfig(abc.ABC):
    @abc.abstractmethod
    def set_up_process(self) -> None: ...

    @abc.abstractmethod
    def tear_down_process(self) -> None: ...

    def __enter__(self) -> None:
        self.set_up_process()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        self.tear_down_process()


class ThreadConfig(abc.ABC):
    @abc.abstractmethod
    def set_up_thread(self) -> None: ...

    @abc.abstractmethod
    def tear_down_thread(self) -> None: ...

    def __enter__(self) -> None:
        self.set_up_thread()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        self.tear_down_thread()
