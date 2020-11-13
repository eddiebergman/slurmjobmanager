"""
Defines the base tasks an Environment should be able to do
"""
# pylint: disable=missing-class-docstring,missing-function-docstring

from typing import Iterable, Protocol, Generic, TypeVar

T = TypeVar('T', contravariant=True)

class Environment(Protocol, Generic[T]):

    def pending_jobs(self) -> Iterable[str]: ...

    def running_jobs(self) -> Iterable[str]: ...

    def complete_jobs(self) -> Iterable[str]: ...

    def queue_job(self, job: T) -> None: ...

    def cancel_job(self, job: T) -> None: ...
