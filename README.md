# Seamstress

A utility for testing concurrent code.

Code that utilizes concurrency concerns is notoriously difficult to write tests
for. `seamstress` makes it a little easier.

## How it Works

The package provides context managers that allow you to run some code in a new
thread, process or async task in your test. The new thread/process/task will
deterministically halt, so that you can "pause" it in any state you desire.
Then, back in your test, you can run other code whose behaviour might be
affected by the state of this new thread/process/task, and make assertions about
how the code behaved.

## API Reference

### Threading utilities

#### `run_thead`

A context manager that creates and runs a new thread. Accepts a context manager
whose context is entered in the new thead, after which control is yielded back
to the calling context. Will block on entry until the `__enter__` method of the
passed context manager has finished executing in the created thread. Similarly,
will block on exit until the `__exit__` method of the passed context manager has
finished executing in the created thread, unless a timeout is exceeded, in which
case an exception will be raised.

Any exception raised during the execution of `context_manager` will be caught
and re-raised in the main thread (i.e. the thread in which the test is running).

##### Arguments

Takes the following parameters:
- `context_manager: typing.ContextManager[None]` - a context manager,
  defining the code to be to be run in the new thread.
- `timeout: float | None` - a timeout, in seconds, for the thread to complete
  executing after the context created by entering `context_manager` is exited.
  Defaults to one second if not specified. If the thread does not complete its
  execution within the passed timeout, `ThreadStillAlive` will be raised within
  the created thread (not the main thread).

##### Signature

~~~python
def run_thread(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.ContextManager[None]:
    ...
~~~

#### `ThreadConfig`

A convenience class, for use with `run_thread`. An abstract base class,
subclasses of which must implement two methods:
- `set_up_thread` run on entering `run_thread`'s context
- `tear_down_thread` run on exiting `run_thread`'s context

Instances of the subclass of `ThreadConfig` can then be passed to the
`context_manager` argument of `run_thread`.

### Multiprocessing utilities

#### `run_process`

A context manager that creates and runs a new process. Accepts a context manager
whose context is entered in the new process, after which control is yielded back
to the calling context. Will block on entry until the `__enter__` method of the
passed context manager has finished executing in the created process. Similarly,
will block on exit until the `__exit__` method of the passed context manager has
finished executing in the created process, unless a timeout is exceeded, in
which case an exception will be raised.

Any exception raised during the execution of `context_manager` will be caught
and re-raised in the main process (i.e. the process in which the test is
running).

##### Arguments

Takes the following parameters:
- `context_manager: typing.ContextManager[None]` - a context manager,
  defining the code to be to be run in a new process.
- `timeout: float | None` - a timeout in seconds for the process to complete
  executing, after the context created by entering `context_manager` is exited.
  Defaults to one second if not specified. If the process does not complete its
  execution within the passed timeout, `ProcessStillAlive` will be raised within
  the created process (not the main process).
- `shared_memory_size: int | None` - you should not usually need to provide this
  value. In order to propagate exceptions raised in the created process back to
  the main process, `seamstress` needs to create some shared memory. Sometimes,
  this memory isn't large enough to hold the raised exception, in which case
  `seamstress` will raise `ExceptionTooLargeToPropagate` in the main process. In
  that exception's error message, it will tell you the value it needs you to
  pass to this argument for the exception to be successfully propagated. This
  should somewhat be considered an implementation detail, but if you want more
  information, look at `seamstress/parallel/_custom_executors.py`, and the tests
  for `run_process`.

##### Signature

~~~python
def run_process(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
    shared_memory_size: int | None = None,
) -> typing.ContextManager[None]:
    ...
~~~

#### `ProcessConfig`

A convenience class, for use with `run_process`. An abstract base class,
subclasses of which must implement two methods:
- `set_up_process` run on entering `run_process`'s context
- `tear_down_process` run on exiting `run_process`'s context

Instances of the subclass of `ProcessConfig` can then be passed to the
`context_manager` argument of `run_process`.

### Async utilities

#### `run_task`

A context manager that creates and runs a new task. Accepts an async context
manager whose context is entered in the new task, after which control is yielded
back to the calling context. On entry, the `__aenter__` method of the passed
async context manager will be run to completion before the code that called
`run_task` cancontinue executing. Similarly, on exit, the `__aexit__` method of the
passed async context manager will be run to completion, unless a timeout is
exceeded, in which case an exception will be raised.

Must be called from within a running event loop, else `NoRunningEventLoop` will
be raised.

##### Arguments

Takes the following parameters:
- `context_manager: typing.AsyncContextManager[None]` - a context manager,
  defining the code to be to be run in a new task.
- `timeout: float | None` - a timeout in seconds for the task to complete
  executing, after the context created by entering `context_manager` is exited.
  Defaults to one second if not specified. If the task does not complete its
  execution within the passed timeout, `TaskStillExecuting` will be raised
  within the created task (not the task that called `run_task`).

##### Signature

~~~python
async def run_task(
    context_manager: typing.AsyncContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.AsyncIterator[None]:
    ...
~~~

#### `TaskConfig`

A convenience class, for use with `run_task`. An abstract base class,
subclasses of which must implement two methods:
- `set_up_task` run on entering `run_task`'s context
- `tear_down_task` run on exiting `run_task`'s context

Instances of the subclass of `TaskConfig` can then be passed to the
`context_manager` argument of `run_task`.

## Examples

### A contested lock

As a simple, slightly contrived example, consider a function `pay_individual`,
which we only want to be called by one thread at a time. If one thread calls the
function whilst it is being executed by another thread, we want an exception to
be thrown. Implementing this could look something like:

~~~python
# inside pay_individual.py
import threading

def _pay_individual(...) -> None:
    # The actual implementation of pay_individual
    ...

class AlreadyPayingIndividual(Exception):
    pass

PAY_INDIVIDUAL_LOCK = threading.Lock()

def pay_individual(...) -> None:
    lock_acquired = PAY_INDIVIDUAL_LOCK.acquire(blocking=False)

    if not lock_acquired:
        raise AlreadyPayingIndividual

    _pay_individual(...)

    PAY_INDIVIDUAL_LOCK.release()

~~~

Say we wanted to write a test to verify that `AlreadyPayingIndividual` is raised
when `pay_individual` is called but `PAY_INDIVIDUAL_LOCK` is acquired. Using
`seamstress` to do so would look something like this:
~~~python
# inside test_pay_individual.py
import unittest

import seamstress

import pay_individual

class PayIndividualLockHogger(seamstress.ThreadConfig):
    """
    Config for a thread that hogs `PAY_INDIVIDUAL_LOCK`, acquiring it for the
    duration of `seamstress.run_thread`'s context.
    """

    def set_up_thread(self) -> None:
        pay_individual.PAY_INDIVIDUAL_LOCK.acquire()

    def tear_down_thread(self) -> None:
        pay_individual.PAY_INDIVIDUAL_LOCK.release()

class TestPayIndividual(unittest.TestCase):
    def test_raises_if_multiple_threads_try_to_pay_individuals(self) -> None:
        with seamstress.run_thread(PayIndividualLockHogger()):
            with self.assertRaises(pay_individual.AlreadyPayingIndividual):
                pay_individual.pay_individual(...)

~~~

Let's break down what happened in the above.
- First, we defined a subclass of `seamstress.ThreadConfig` called
    `PayIndividualLockHogger` (`ThreadConfig` is a convenience class that
    `seamstress` provides, for ease of setting up threads in a certain state).
- Then, we passed an instance of `PayIndividualLockHogger` to
  `seamstress.run_thread`, whilst entering its context. Under the bonnet,
  `seamstress` created a new thread that ran
  `PayIndividualLockHogger.set_up_thread` (and so acquired
  `PAY_INDIVIDUAL_LOCK`), before letting the test resume execution.
- We then entered unittest's `assertRaises`
    [helper](https://docs.python.org/3/library/unittest.html#unittest.TestCase.assertRaises),
    so that our test can verify that `pay_individual` raises if called whilst
    `PAY_INDIVIDUAL_LOCK` is acquired.
- From within `assertRaises`, we call `pay_individual`, which raises
    `AlreadyPayingIndividual` because `PAY_INDIVIDUAL_LOCK` has been acquired
    from the thread that seamstress created. So we exit the `assertRaises` block
    without the test failing.
- Finally, we exit `seamstress.run_thread`. Under the bonnet,
  `PayIndividualLockHogger.tear_down_thread` runs in the created thread, and so
  it releases `PAY_INDIVIDUAL_LOCK`, and doesn't pollute any other tests.
  `seamstress.run_thread` then calls `.join()` on the thread, waiting for it to
  terminate.

If `pay_individual` used `multiprocessing.Lock`, the above test would be the
same, but would use `seamstress.run_process` and `seamstress.ProcessConfig`
instead.

`seamstress.run_thread` can also be used as a decorator, so if we wanted to save
a layer of intendation we could also have written the test as:

~~~python
class TestPayIndividual(unittest.TestCase):
    @seamstress.run_thread(PayIndividualLockHogger())
    def test_raises_if_multiple_threads_try_to_pay_individuals(self) -> None:
        with self.assertRaises(pay_individual.AlreadyPayingIndividual):
            pay_individual.pay_individual(...)
~~~

#### Aside: `run_thread` only cares about being handed a context manager

The class `seamstress.ThreadConfig` is only really provided for convenience/code
clarity. `run_thread` accepts being passed any context manager, running its
`__enter__` method on entry, and its `__exit__` method on exit. We could have
manually built a context manager that acquires/releases `PAY_INDIVIDUAL_LOCK` on
entry/exit ourselves, and `seamstress` would've behaved exactly the same:

~~~python
# inside test_pay_individual.py
import types
import unittest

import seamstress

import pay_individual

class PayIndividualLockHogger:
    def __enter__(self) -> None:
        pay_individual.PAY_INDIVIDUAL_LOCK.acquire()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        pay_individual.PAY_INDIVIDUAL_LOCK.release()

class TestPayIndividual(unittest.TestCase):
    # as before
    ...

~~~

Taking this further, `threading.Lock` is itself is a context manager, and so
could have been passed directly to `run_thread` with the same behaviour
resulting:

~~~python
# inside test_pay_individual.py
import unittest

import seamstress

import pay_individual

class TestPayIndividual(unittest.TestCase):
    def test_raises_if_multiple_threads_try_to_pay_individuals(self) -> None:
        with seamstress.run_thread(pay_individual.PAY_INDIVIDUAL_LOCK):
            with self.assertRaises(pay_individual.AlreadyPayingIndividual):
                pay_individual.pay_individual(...)

~~~

Though perhaps this is a little less clear than using `ThreadConfig`.

### Acquiring a Database Lock

Say we had some django code that only runs if it can acquire an advisory lock:

~~~python
# inside pay_individual.py
import pglock

from django.db import transaction

def _pay_individual(...) -> None:
    # The actual implementation of pay_individual
    ...

class AlreadyPayingIndividual(Exception):
    pass

def _get_advisory_lock_name_for_pay_individual(
    *,
    individual: models.Individual,
) -> str:
    return f"pay-individual-{individual.id}"

def pay_individual(
    *,
    individual: models.Individual,
    ...,
) -> None:
    with transaction.atomic():
        lock_acquired = pglock.advisory(
            _get_advisory_lock_name_for_pay_individual(individual=individual),
            xact=True,
            timeout=0,
        ).acquire()

        if not lock_acquired:
            raise AlreadyPayingIndividual

        _pay_individual(...)

~~~

Testing this code is non-trivial, as it's not possible to open multiple database
transactions from a thread with the utilities that django provides. `seamstress`
makes opening multiple database transactions and testing advisory lock handling
more straightforward:

~~~python
# inside test_pay_individual.py
import contextlib
import typing

import seamstress
import pglock

from django.db import transaction, close_old_connections
from django.test import TestCase

import models
import pay_individual


def build_pay_individual_lock_hogger(
    self,
    *,
    individual: models.Individual,
) -> typing.ContextManager[None]:
    # define a lock hogger that acquires the advisory lock from a transaction
    # opened in a different thread, then yields for the test to resume operation
    @contextlib.contextmanager
    def pay_individual_lock_hogger():
        try:
            with transaction.atomic():
                pglock.advisory(
                    pay_individual._get_advisory_lock_name_for_pay_individual(
                        individual=individual,
                    ),
                    xact=True,
                    timeout=0,
                ).acquire()
                yield
        finally:
            # close old connections, just to be safe/make sure our extra thread doesn't lead
            # to dangling connections
            close_old_connections()

    return pay_individual_lock_hogger()

class TestPayIndividual(TestCase):
    def test_raises_individual_already_being_paid(self) -> None:
        individual = models.Individual.objects.create(...)

        pay_individual_lock_hogger = build_pay_individual_lock_hogger(
            individual=individual
        )

        with seamstress.run_thread(pay_individual_lock_hogger):
            with self.assertRaises(pay_individual.AlreadyPayingIndividual):
                pay_individual.pay_individual(...)

~~~

Interestingly, this means you can test locking behaviours without having to use
`TransactionTestCase` (because the transaction that acquires the lock is opened in a
different thread to the one that is running the test).

This example was the real-world situation that prompted the writing of this package.

The above code can be tweaked pretty minimally for wherever you need to test
code that behaves differently depending on whether or not a lock is acquired.
For example, you could use it to test code that uses:
- Advisory locks in databases other than postgres (i.e. not using `pglock` as in the
  above example)
- UNIX file/io locks, using `fcntl`
- A distributed redis lock, using `redis.lock.Lock`

### Running tasks

In analogy to `run_thread` and `run_process` an asynchronous utility called
`run_task` is also provided. Its API is very similar.

Say that `pay_individual` from the first example was actually an asynchronous
function:
~~~python
# inside pay_individual.py
import asyncio

async def _pay_individual(...) -> None:
    # The actual implementation of pay_individual
    ...

class AlreadyPayingIndividual(Exception):
    pass

PAY_INDIVIDUAL_LOCK = asyncio.Lock()

async def pay_individual(...) -> None:
    try:
        async with asyncio.timeout(0.1):
            await PAY_INDIVIDUAL_LOCK.acquire()
    except asyncio.TimeoutError as e:
        raise AlreadyPayingIndividual from e

    await _pay_individual(...)

    PAY_INDIVIDUAL_LOCK.release()

~~~

Using `run_task` and `TaskConfig` to test its behaviour may look something
like:
~~~python
# inside test_pay_individual.py
import unittest

import seamstress

import pay_individual

class PayIndividualLockHogger(seamstress.TaskConfig):
    async def set_up_task(self) -> None:
        await pay_individual.PAY_INDIVIDUAL_LOCK.acquire()

    async def tear_down_task(self) -> None:
        pay_individual.PAY_INDIVIDUAL_LOCK.release()

class TestPayIndividual(unittest.IsolatedAsyncioTestCase):
    async def test_raises_if_multiple_tasks_try_to_pay_individuals(self) -> None:
        async with seamstress.run_task(PayIndividualLockHogger()):
            with self.assertRaises(pay_individual.AlreadyPayingIndividual):
                await pay_individual.pay_individual(...)

~~~

## Contributing to `seamstress`

### Running tests

Tests can be run from within the virtual environment using:
~~~bash
python3 -m unittest
~~~

Alternatively, tests can be run using `uv`:
~~~bash
uv run python3 -m unittest
~~~

Before running tests, you may need to run either:
~~~bash
python3 -m pip install -e .
~~~
or:
~~~bash
uv pip install -e .
~~~
in order to install `seamstress` in development/editable mode (though `uv` might
have automatically done this for you).

Please ensure all PRs have appropriate test coverage.

## Why "seamstress"?

A seamstress stitches threads together for you, which is what this package does
too!

## Avenues for Improvement

### Set up `pre-commit` (or similar) for local development

Tests and type-checking are currently run on `main` in github. It'd be neat if
they could be run on-commit too, using something like pre-commit (given they
only take a few seconds at the moment).
