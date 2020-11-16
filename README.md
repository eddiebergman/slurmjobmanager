A small module to help with managing Slurm and jobs through Python,
quite useful in conjuction with `IPython` for interactive usage.

## Installation
```BASH
git clone https://github.com/eddiebergman/slurmjobmanager
cd slurmjobmanager
pip install .
```

If you want to have the package installed while being able to edit it and
not have to constantly reinstall it use `editable` command
```BASH
pip install -e .
```

## Usage
The library has two main classes:
* `SlurmEnvironment`
    Manages retrieving information from Slurm about status of jobs as well
    as queuing and cancelling jobs.

* `SlurmJob`
    An abstract class defining what is required for a job to be managed by
    `slurmjobmanager`. See the source code for a list of `@abstractmethods` that
    need to be implemented.

```Python
from slurmjobmanager import SlurmEnvironment, SlurmJob

slurm_env = SlurmEnvironment('username')

class MyJobType(SlurmJob):
    """ ...implements all the abstract methods of SlurmJob """
    ...

def create_jobs(env, config) -> List[MyJobType]:
    ...

myjobs = create_jobs(slurm_env, 'some_config.json')
for job in myjobs:
    job.queue()

print(slurm_env.in_progress_jobs())

job_I_want_to_cancel = myjobs[2]
slurm_env.cancel_job(job_I_want_to_cancel)

slurm_env.cancel_by_name('some_job_name')

print(slurm_env.jobs_info())
```

Please see the relatively small source code for a full list of methods available
for both `SlurmJob` and `SlurmEnvironment`.
