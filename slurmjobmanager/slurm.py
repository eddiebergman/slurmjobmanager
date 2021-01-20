"""
Module to handle Slurm related jobs
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import os
from abc import abstractmethod, ABC
from typing import Mapping, Iterable, List, Dict, Any
from subprocess import check_output

from .environment import Environment
from .job import Job

class SlurmJob(Job, ABC):
    """
    An abstract class that describes whats required for job to be run
    in a SlurmEnvironment. Inheriting this class and implementing the abstract
    methods should be enough to allow that class of Job to be run on Slurm
    """

    @abstractmethod
    def name(self) -> str:
        """"
        Returns the name of this jobs

        Note:  SLURM truncates names around the ~20 char limit
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
    def slurm_args(self) -> Mapping[str, Any]:
        """ Provides the arguments for Slurm's sbatch """
        raise NotImplementedError

    @abstractmethod
    def slurm_opts(self) -> Iterable[str]:
        """ Provides any options for Slurm's sbatch """
        raise NotImplementedError

    @abstractmethod
    def slurm_command(self) -> str:
        """ The actual command to be run by sbatch """
        raise NotImplementedError

    @abstractmethod
    def slurm_path(self) -> str:
        """ The location of the slurm script to run """
        raise NotImplementedError

    @abstractmethod
    def setup(self) -> None:
        """ Any setup that must be done before a job is queued """
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        """ Reset the job to allow it to be run again """
        raise NotImplementedError

    def create_slurm_config(self) -> None:
        """
        Creates the slurmscript so the job can be run in the SlurmEnvironment
        This will ask the job to do its setup() first.
        """
        self.setup()

        args = self.slurm_args()
        opts = self.slurm_opts()
        command = self.slurm_command()
        script_path = self.slurm_path()

        with open(script_path, 'w') as outfile:
            outfile.writelines('#!/bin/bash\n')
            for key, val in args.items():
                outfile.writelines(f'#SBATCH --{key}={val}\n')
            for opt in opts:
                outfile.writelines(f'#SBATCH --{opt}\n')

            outfile.writelines(command)

class SlurmEnvironment(Environment[SlurmJob]):
    """
    Communicates with slurm for information on jobs.
    May have to use the refresh functionality if using in
    an interactive context.
    """

    def __init__(
        self,
        username: str,
    ) -> None:
        """
        Params
        ======
        username: str
            The username used for the Slurm Cluster
        """
        super().__init__()
        self.username = username
        self._info : Dict[str, List[str]] = {}

    def pending_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            A list of jobs with the status pending 'P'
        """
        info = self.info()
        return info['pending']

    def running_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            A list of jobs with the status pending 'R'
        """
        info = self.info()
        return info['running']

    def unknown_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            returns the list of jobs with a status other than pending or running
        """
        info = self.info()
        return info['unknown']

    def cancel_by_name(self, name: str) -> None:
        """
        Params
        ======
        name: str
            Cancels by the name given to the job
        """
        os.system(f'scancel -n {name}')

    def cancel_jobs(self, jobs: Iterable[SlurmJob]) -> None:
        """
        Params
        ======
        job : SlurmJob
            The job to cancel
        """
        for job in jobs:
            self.cancel_by_name(job.name())

    def run_job(self, job: SlurmJob, force: bool = False):
        self.queue_job(job, force)

    def queue_job(
        self,
        job: SlurmJob,
        force: bool = False
      ) -> None:
        """
        Must include either job-object or a name.
        Will fail if the job is not ready to be run or if it is already
        in progress or finished.

        By specifying force, the job can be requeued if it is ready. This
        performs a reset and cancels the job if it is in progress.

        Params
        ======
        job : SlurmJob
            The job to cancel

        force: bool = False
            Whether to force a requeue if the job is in progress or completed.
            Note: Will perform a job reset
        """
        in_progress_jobs = self.running_jobs() + self.pending_jobs()
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

        if job.name() in in_progress_jobs:
            if force:
                self.cancel_by_name(job.name())
                job.reset()
            else:
                raise RuntimeError(f'Job {job.name()} in progress, force a'
                                   + ' cancel, reset and requeue with'
                                   + ' `force=True`')

        if job.failed():
            if force:
                job.reset()
            else:
                raise RuntimeError(f'Job {job.name()} has already finished and'
                                   + ' has failed, force a reset and requeue'
                                   + ' with `force=True`')

        slurm_script_path = job.slurm_path()
        if not os.path.exists(slurm_script_path):
            job.create_slurm_config()

        os.system(f'sbatch {slurm_script_path}')

    def info(self, refresh=False) -> Dict[str, Any]:
        """
        Returns
        =======
        Dict[str, List[str]]
            A dictionary with the keys 'running', 'pending', 'unknown' depending
            on the status of the jobs known to Slurm.
        """
        if self._info == {} or refresh:
            self.refresh_info()
        return self._info

    def refresh_info(self) -> None:
        """
        Calls 'squeue' to get information about your jobs in the queue and
        populates the info.

        This is cached to prevent iterative calls, when using this module
        in an interactive session like IPython, you may want to manually
        call this if you think a job may have changed status.
        """
        command = f'squeue -u {self.username} -h -o %t-%j'.split(' ')
        raw_info = check_output(command).decode('utf-8').split('\n')

        info : Dict[str, List[str]] = {
            'running' : [],
            'pending' : [],
            'unknown' : [],
        }
        categories : Dict[str, str] = {
            'R' : 'running',
            'PD': 'pending',
            '--': 'unknown',
        }
        for line in raw_info:
            if line != '':
                status, jobname = tuple(line.split('-'))
                category = categories.get(status, 'unknown')
                info[category].append(jobname)

        self._info = info
