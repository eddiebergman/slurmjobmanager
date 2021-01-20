"""
Defines the base tasks an Environment should be able to do
"""
# pylint: disable=missing-class-docstring,missing-function-docstring

from typing import Protocol, Generic, TypeVar, Dict, Any

T = TypeVar('T', contravariant=True)

class Environment(Protocol, Generic[T]):

    def run_job(self, job: T) -> None: ...

    def info(self) -> Dict[str, Any]: ...
