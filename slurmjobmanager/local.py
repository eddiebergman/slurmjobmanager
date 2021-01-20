"""
Defines running in a local environment
Not very effective but imitates what's used for slurm incase
of running in local scenarios.
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import os
from abc import abstractmethod, ABC
from typing import List, Dict, Any

from .environment import Environment
from .job import Job

class LocalJob(Job, ABC):

    @abstractmethod
    def name(self) -> str:
        """"
        Returns the name of this jobs
        """
        raise NotImplementedError

    @abstractmethod
    def complete(self) -> bool:
        """ Indicates whether this job is complete or not """
        raise NotImplementedError

    @abstractmethod
    def ready(self) -> bool:
        """ Wether the job is ready to be queued """
        raise NotImplementedError

    @abstractmethod
    def failed(self) -> bool:
        """ Indicate whether this job has ran and failed """
        raise NotImplementedError

    @abstractmethod
    def blocked(self) -> bool:
        """ Indicate whether this job can not be run due to dependancies """
        raise NotImplementedError

    @abstractmethod
    def command(self) -> str:
        """ The actual command to be run """
        raise NotImplementedError

    @abstractmethod
    def setup(self) -> None:
        """ Any setup that must be done before a job is queued """
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        """ Reset the job to allow it to be run again """
        raise NotImplementedError

class LocalEnvironment(Environment[LocalJob]):
    """
    Works like an environemnt to have consistency between local and
    slurm environments. Due to the variance in systems, provides very little
    functionallity other than just running a job as a command.
    """

    def __init__(self) -> None:
        super().__init__()
        self.jobs_run : List[LocalJob] = []

    def run_job(
        self,
        job: LocalJob,
        force: bool = False
      ) -> None:
        """
        Simply runs the command associated with this job. Care must be
        taken if running this code under multiple processes as the same
        job may be run concurrently, leading to issues.

        By specifying force, the job can be run if it is ready.

        Params
        ======
        job : LocalJob
            The job to cancel

        force: bool = False
            Whether to force a requeue if the job is in progress or completed.
            Note: Will perform a job reset
        """
        if job.blocked():
            raise RuntimeError(f'Job {job.name()} has indicated it is currently'
                               + 'blocked')

        if not job.ready():
            raise RuntimeError(f'Job {job.name()} has indicated it is not ready')

        if job.complete():
            if force:
                job.reset()
            else:
                raise RuntimeError(f'Job {job.name()} already complete, force a'
                                   + ' reset and requeue with `force=True`')

        if job.failed():
            if force:
                job.reset()
            else:
                raise RuntimeError(f'Job {job.name()} has already finished and'
                                   + ' has failed, force a reset and requeue'
                                   + ' with `force=True`')

        job.setup()
        os.system(f'{job.command()}')
        self.jobs_run.append(job)

    def info(self) -> Dict[str, Any]:
        return {'jobs_run' : self.jobs_run}
