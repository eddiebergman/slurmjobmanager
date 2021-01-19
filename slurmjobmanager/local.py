"""
Defines running in a local environment
Not very effective but imitates what's used for slurm incase
of running in local scenarios.
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import os
from abc import abstractmethod
from typing import List

from .environment import Environment
from .job import Job

class LocalJob(Job):
    """
    Defines a job that is to be run locally, does not provide the following
    methods:
     - 'in_progress'
     - 'running'
     - 'cancel'
    This is due to the variability of local running environments.
    """

    def __init__(self, environment: LocalEnvironment,  *args, **kwargs):
        """

        Params
        ======
        env: LocalEnvironment
            A local environment to run in
        """
        super().__init__()
        self.env = environment

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

    def pending(self) -> bool:
        """
        LocalJob's are considered to not be pending and run when a command is 
        set.

        Returns
        =======
        false: bool
        """
        return False

    def running(self) -> bool:
        """
        Can not determine if a job is running in local environment.

        Returns
        =======
        ERROR
        """
        raise RuntimeError('LocalJob and LocalEnvironment do not provide'
                           + ' information on running or in progress jobs.')

    def in_progress(self) -> bool:
        """"
        Can not determine if a job is running in local environment.

        Returns
        =======
        ERROR
        """
        raise RuntimeError('LocalJob and LocalEnvironment do not provide'
                           + ' information on running or in progress jobs.')

    def run(self, force: bool = False) -> None:
        """
        Params
        ======
        force: bool = False
            Whether to force the job in the case it is already in progress,
            completed or failed. This will ask the job to reset itself so it
            can be run again.
        """
        self.env.queue_job(self, force=force)

    def cancel(self) -> None:
        """"
        Can not cancel a job in localenvironment

        Returns
        =======
        ERROR
        """
        raise RuntimeError('LocalJob and LocalEnvironment do not job canceling,'
                           + ' use system method of ending process')

class LocalEnvironment(Environment[LocalJob]):
    """
    Works like an environemnt to have consistency between local and
    slurm environments. Due to the variance in systems, provides very little
    functionallity other than just running a job as a command.
    """

    def __init__(self) -> None:
        super().__init__()

    def pending_jobs(self) -> List[str]:
        """
        Cannot query pending jobs in LocalEnvironment

        Returns
        =======
        ERROR
        """
        raise RuntimeError('LocalEnvironment does not provide'
                           + ' information on jobs.')

    def running_jobs(self) -> List[str]:
        """
        Cannot query running jobs in LocalEnvironment

        Returns
        =======
        ERROR
        """
        raise RuntimeError('LocalEnvironment does not provide'
                           + ' information on jobs.')

    def in_progress_jobs(self) -> List[str]:
        """
        Cannot query if job status is unknown in LocalEnvironment

        =======
        ERROR
        """
        raise RuntimeError('LocalEnvironment does not provide'
                           + ' information on jobs.')

    def cancel_by_name(self, name: str) -> None:
        """
        Can not cancel in LocalEnvironment
        """
        raise RuntimeError('LocalEnvironment does not provide job cancellation,'
                           + ' use system method to end this process.')

    def cancel_job(self, job: LocalJob) -> None:
        """
        Can not cancel in LocalEnvironment
        """
        raise RuntimeError('LocalEnvironment does not provide job cancellation,'
                           + ' use system method to end this process.')

    def queue_job(
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
