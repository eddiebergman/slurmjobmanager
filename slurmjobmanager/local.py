"""
Defines running in a local environment
Not very effective but imitates what's used for slurm incase
of running in local scenarios.
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import subprocess
from typing import List, Dict, Any, Mapping

from .environment import Environment
from .job import Job

class LocalEnvironment(Environment):
    """
    Works like an environemnt to have consistency between local and
    slurm environments. Due to the variance in systems, provides very little
    functionallity other than just running a job as a command.
    """

    def __init__(self) -> None:
        super().__init__()
        self.jobs_run : List[Job] = []

    def run(
        self,
        job: Job,
        options: Mapping[str, Any]
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
        force = options.get('force', False)
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

        job.setup()

        subprocess_default_args = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.STDOUT,
            'text': True,
        }
        subprocess_args = {**subprocess_default_args, **options}
        command = job.command().split(' ')

        # On getting continuous output
        # https://stackoverflow.com/a/4417735/5332072
        process = subprocess.Popen(command, **subprocess_args)
        for line in iter(process.stdout.readline, ""):
            yield line
        process.stdout.close()
        return_code = process.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, command)

        self.jobs_run.append(job)

    def info(self) -> Dict[str, Any]:
        return {'jobs_run' : self.jobs_run}

