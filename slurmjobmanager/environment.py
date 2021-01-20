"""
Defines the base tasks an Environment should be able to do
"""
# pylint: disable=missing-class-docstring,missing-function-docstring

from typing import Protocol, Dict, Any, Mapping
from .job import Job

class Environment(Protocol):

    def run(self, job: Job, options: Mapping[str, Any]) -> None: ...

    def info(self) -> Dict[str, Any]: ...
