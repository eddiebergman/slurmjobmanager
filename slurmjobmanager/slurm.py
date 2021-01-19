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
    def __init__(self, environment: SlurmEnvironment, *args, **kwargs):
        """
        Params
        ======
        environment: SlurmEnvironment
            The slurm environment to be run in
        """
        super().__init__()
        self.env = environment

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
    def slurm_script_path(self) -> str:
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

    def pending(self) -> bool:
        """
        Returns
        =======
        bool
            Whether the job is pending in the SlurmEnvironment
        """
        pending_jobs = self.env.pending_jobs()
        return self.name() in pending_jobs

    def running(self) -> bool:
        """
        Returns
        =======
        bool
            Whether the job is running in the SlurmEnvironment
        """
        running_jobs = self.env.running_jobs()
        return self.name() in running_jobs

    def unknown(self) -> bool:
        """
        Returns
        =======
        bool
            Whether the status of this job is unknown in the SlurmEnvironment
        """
        unknown_jobs = self.env.unknown_jobs()
        return self.name() in unknown_jobs

    def in_progress(self) -> bool:
        """
        Returns
        =======
        bool:
            Whether the job is currently in progress in the SlurmEnvironment
        """
        return self.pending() or self.running()

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
        """ Cancels the job if it is queued, has no effect otherwise """
        self.env.cancel_job(self)

    def create(self) -> None:
        """
        Creates the slurmscript so the job can be run in the SlurmEnvironment
        This will ask the job to do its setup() first.
        """
        self.setup()

        args = self.slurm_args()
        opts = self.slurm_opts()
        command = self.slurm_command()
        script_path = self.slurm_script_path()

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

    def __init__(self, username: str) -> None:
        """
        Params
        ======
        username: str
            The username used for the Slurm Cluster
        """
        super().__init__()
        self.username = username
        self._jobs_info : Dict[str, List[str]] = {}

    def pending_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            A list of jobs with the status pending 'P'
        """
        jobs_info = self.jobs_info()
        return jobs_info['pending']

    def running_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            A list of jobs with the status pending 'R'
        """
        jobs_info = self.jobs_info()
        return jobs_info['running']

    def unknown_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            returns the list of jobs with a status other than pending or running
        """
        jobs_info = self.jobs_info()
        return jobs_info['unknown']

    def in_progress_jobs(self) -> List[str]:
        """
        Returns
        =======
        List[str]:
            A list of any jobs that are pending or running
        """
        return self.pending_jobs() + self.running_jobs()

    def cancel_by_name(self, name: str) -> None:
        """
        Params
        ======
        name: str
            Cancels by the name given to the job
        """
        os.system(f'scancel -n {name}')

    def cancel_job(self, job: SlurmJob) -> None:
        """
        Params
        ======
        job : SlurmJob
            The job to cancel
        """
        self.cancel_by_name(job.name())

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

        if job.in_progress():
            if force:
                job.cancel()
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

        slurm_script_path = job.slurm_script_path()

        if not os.path.exists(slurm_script_path):
            job.create()

        os.system(f'sbatch {slurm_script_path}')

    def jobs_info(self) -> Dict[str, List[str]]:
        """
        Returns
        =======
        Dict[str, List[str]]
            A dictionary with the keys 'running', 'pending', 'unknown' depending
            on the status of the jobs known to Slurm.
        """
        if self._jobs_info == {}:
            self.refresh_info()
        return self._jobs_info

    def refresh_info(self) -> None:
        """
        Calls 'squeue' to get information about your jobs in the queue and
        populates the info.

        This is cached to prevent iterative calls, when using this module
        in an interactive session like IPython, you may want to manually
        call this if you think a job may have changed status.
        """
        command = f'squeue -u {self.username} -h -o %t-%j'.split(' ')
        info = check_output(command).decode('utf-8').split('\n')

        _jobs_info : Dict[str, List[str]] = {
            'running' : [],
            'pending' : [],
            'unknown' : [],
        }
        categories : Dict[str, str] = {
            'R' : 'running',
            'PD': 'pending',
            '--': 'unknown',
        }
        for line in info:
            if line != '':
                status, jobname = tuple(line.split('-'))
                category = categories.get(status, 'unknown')
                _jobs_info[category].append(jobname)

        self._jobs_info = _jobs_info

    def slurm_time_and_partition(
        self,
        time: int,
        buffer: float = 0.25
    ) -> Dict[str, str]:
        """
        Params
        ======
        time: int
            The amount of time in minutes for the job

        buffer: float
            The percent of extra time to add on as a buffer

        Returns
        =======
        Dict[str, str]
            The partition to use and the allocated time as a str
        """
        allocated_time = int(time * (1 + buffer))
        d = int(allocated_time / (60*24))
        h = int((allocated_time - d * 24 * 60) / 60)
        m = int(allocated_time % 60)

        short_max = (2 * 60)
        medium_max = (1 * 24 * 60)
        defq_max = (4 * 24 * 60)
        # zfill(2) just pre-pads with 0's to a str length of 2
        time_str = f'{d}-{str(h).zfill(2)}:{str(m).zfill(2)}:00'
        if allocated_time < short_max:
            return {'partition': 'short', 'time': time_str }
        elif allocated_time < medium_max:
            return {'partition': 'medium', 'time': time_str }
        elif allocated_time < defq_max:
            return {'partition': 'defq', 'time': time_str }
        else:
            raise ValueError('Requires too much time,'
                             + f'{allocated_time}, possible to queue'
                             + 'on the smp partition if required')
