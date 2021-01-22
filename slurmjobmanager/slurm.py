"""
Module to handle Slurm related jobs
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import os
from typing import Mapping, Iterable, List, Dict, Any, Optional
from subprocess import check_output

from .environment import Environment
from .job import Job

class SlurmEnvironment(Environment):
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

    def cancel_job(self, job: Job) -> None:
        """
        Params
        ======
        job : Job
            The job to cancel
        """
        self.cancel_by_name(job.name())

    def run(
        self,
        job: Job,
        options: Mapping[str, Any],
    ) -> None:
        slurm_args = options['slurm_args']
        slurm_script_path = options['slurm_script_path']
        slurm_opts = options.get('slurm_opts', [])
        force = options.get('force', False)

        self.queue(job,
                   args=slurm_args,
                   script_path=slurm_script_path,
                   opts=slurm_opts,
                   force=force)

    def queue(
        self,
        job: Job,
        args: Mapping[str, Any],
        script_path: str,
        opts: Iterable[str],
        force: bool = False
      ) -> None:
        """
        Must include either job-object
        Will fail if the job is not ready to be run or if it is already
        in progress or finished.

        By specifying force, the job can be requeued if it is ready. This
        performs a reset and cancels the job if it is in progress.

        Params
        ======
        job : Job
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
            else:
                raise RuntimeError(f'Job {job.name()} in progress, force a' + ' cancel, reset and requeue with'
                                   + ' `force=True`')

        if force:
            job.reset()

        if not os.path.exists(script_path):
            self.create_slurm_script(args=args,
                                     command=job.command(),
                                     script_path=script_path,
                                     opts=opts)

        job.setup()

        os.system(f'sbatch {script_path}')

    def info(self) -> Dict[str, Any]:
        """
        Returns
        =======
        Dict[str, List[str]]
            A dictionary with the keys 'running', 'pending', 'unknown' depending
            on the status of the jobs known to Slurm.
        """
        if self._info == {}:
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

    def create_slurm_script(
        self,
        args : Mapping[str, Any],
        command : str,
        script_path: str,
        opts: Optional[Iterable[str]] = None
    ) -> None:
        """
        Creates the slurmscript so the job can be run in the SlurmEnvironment
        """
        opts = [] if opts is None else opts
        with open(script_path, 'w') as outfile:
            outfile.writelines('#!/bin/bash\n')
            for key, val in args.items():
                outfile.writelines(f'#SBATCH --{key}={val}\n')
            for opt in opts:
                outfile.writelines(f'#SBATCH --{opt}\n')

            outfile.writelines(command)
