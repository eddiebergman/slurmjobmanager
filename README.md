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
    as queuing and cancelling jobs. It will also query any jobs for 
    slurm arguements and create the associated batch file.

* `SlurmJob`
    An abstract class defining what is required for a job to be managed by
    `slurmjobmanager`. See the source code for a list of `@abstractmethods` that
    need to be implemented.

```Python
from slurmjobmanager import SlurmEnvironment, SlurmJob

slurm_env = SlurmEnvironment('username')

class MyJobType(SlurmJob):
    ...

def create_jobs(env) -> List[MyJobType]:
    ...

myjobs = create_jobs(slurm_env)
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

## Implementing SlurmJob Example
Here's some simplified code I am using for the TPOT Benchmark that should give an idea
of how job classes can be structured.

```Python
class TPOTJob(SlurmJob):

    def __init__(self, env, seed, task, times, benchdir, python_script):
        super().__init__(env)
        self.seed = seed
        self.task = task
        self.times = sorted(times)
        self.benchdir = benchdir
        self.python_script = python_script
        self.folders = { ... }
        self.files = { ... }

    def name(self):
        return f'{self.seed}_{self.task}_etc...'

    def complete(self):
        files = self.files['classifications'] + self.files['predictions']
        return all(os.path.exists(file) for file in files)

    def ready(self):
        return not self.blocked()

    def failed(self):
        if (
            not self.complete()
            and os.path.exists(self.files['slurm_out'])
            and not self.in_progress()
        ):
            return True
        else:
            return False

    def blocked(self):
        # No dependancies
        return False

    def setup(self):
        if not os.path.exists(self.folders['root']):
            os.mkdir(self.folders['root'])

        for path in self.folders.values():
            if not os.path.exists(path):
                os.mkdir(path)

        config = { ... }
        with open(self.files['config'], 'w') as f:
            json.write(config, f)

    def reset(self):
        rmtree(self.folders['root'])

    def slurm_args(self):
        time = max(self.times)
        return {
            'job-name': self.name(),
            'nodes': 1,
            'ntasks': 1,
            'cpus-per-task': 4,
            'output': self.files['slurm_out'],
            'error': self.files['slurm_err']
            'export': 'ALL',
            'mem': 16000,
            **self.env.slurm_time_and_partition(time, buffer=0.25)
        }

    def slurm_opts(self):
        return []

    def slurm_script_path(self):
        return self.files['slurm_script']

    def slurm_command(self):
        return f'python {self.python_script} {self.files['config']}'
```
