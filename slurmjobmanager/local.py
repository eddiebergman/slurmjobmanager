"""
Defines running in a local environment
Not very effective but imitates what's used for slurm incase
of running in local scenarios.
"""
# See https://docs.python.org/dev/whatsnew/3.10.html#new-features
from __future__ import annotations

import os
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
        os.system(f'{job.command()}')
        self.jobs_run.append(job)

    def info(self) -> Dict[str, Any]:
        return {'jobs_run' : self.jobs_run}
