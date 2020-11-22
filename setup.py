import os
import setuptools
import sys

if sys.version_info < (3, 7):
    raise ValueError(f"Unsupported Python version {sys.version_info.major}."
                     + f"{sys.version_info.micro}.{sys.version_info.micro}"
                     + f"found. Requires Python 3.5 or higher."
    )

with open(os.path.join("README.md")) as fid:
    README = fid.read()

setuptools.setup(
    name="slurmjobmanager",
    author="Eddie Bergman",
    author_email="Edward.Bergman@uni-siegen.de",
    maintainer="Eddie Bergman",
    maintainer_email="Edward.Bergman@uni-siegen.de",
    description="A Python module for interacting with a Slurm cluster",
    long_description=README,
    long_description_content_type="text/markdown",
    license="BSD 3-clause",
    version="0.1.0",
    packages=setuptools.find_packages(),
    package_data={'slurmjobmanager': ['py.typed']},
    python_requires=">=3.7",
    install_requires=[],
    extras_require={
        "test": [
            "mypy",
            "pylint",
        ],
    },
)

