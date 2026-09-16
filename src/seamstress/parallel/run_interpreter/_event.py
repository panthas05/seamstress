from __future__ import annotations

import sys

if sys.version_info < (3, 14):
    error_message = "Requires Python 3.14+"
    raise RuntimeError(error_message)


import os


class Waitable:
    """
    An object that can be waited upon. To be used in conjunction with a `Settable`
    instance to mimic an `Event` from python's threading/multiprocessing modules (will
    block until `.set()` has been called on a `Settable` instance constructed with the
    corresponding write file descriptor).
    """

    _read_file_descriptor: int

    def __init__(self, *, read_file_descriptor: int) -> None:
        self._read_file_descriptor = read_file_descriptor

    def wait(self) -> None:
        os.read(self._read_file_descriptor, 1)
        os.close(self._read_file_descriptor)

    @property
    def read_file_descriptor(self) -> int:
        return self._read_file_descriptor


class Settable:
    """
    An object that can be waited upon. To be used in conjunction with a `Waitable`
    instance to mimic an `Event` from python's threading/multiprocessing modules (calls
    to the `.wait()` method of a `Waitable` instance constructed with the corresponding
    read file descriptor will block until `.set()` has been called on the `Settable`
    instance).
    """

    _write_file_descriptor: int

    def __init__(self, *, write_file_descriptor: int) -> None:
        self._write_file_descriptor = write_file_descriptor

    def set(self) -> None:
        os.write(self._write_file_descriptor, b"\x01")
        os.close(self._write_file_descriptor)


class Event:
    _waitable: Waitable
    _settable: Settable

    _is_set: bool

    def __init__(
        self,
        *,
        read_file_descriptor: int,
        write_file_descriptor: int,
    ) -> None:
        self._waitable = Waitable(read_file_descriptor=read_file_descriptor)
        self._settable = Settable(write_file_descriptor=write_file_descriptor)

        self._is_set = False

    def wait(self) -> None:
        return self._waitable.wait()

    def is_set(self) -> bool:
        return self._is_set

    def set(self) -> None:
        self._is_set = True
        return self._settable.set()

    @property
    def read_file_descriptor(self) -> int:
        return self._waitable.read_file_descriptor


def build_event() -> Event:
    (
        read_file_descriptor,
        write_file_descriptor,
    ) = os.pipe()
    return Event(
        read_file_descriptor=read_file_descriptor,
        write_file_descriptor=write_file_descriptor,
    )
