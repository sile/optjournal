import os

from setuptools import find_packages
from setuptools import setup

from typing import Dict
from typing import List


def get_version() -> str:

    version_filepath = os.path.join(os.path.dirname(__file__), "optjournal", "version.py")
    with open(version_filepath) as f:
        for line in f:
            if line.startswith("__version__"):
                return line.strip().split()[-1][1:-1]
    assert False


def get_long_description() -> str:

    readme_filepath = os.path.join(os.path.dirname(__file__), "README.md")
    with open(readme_filepath) as f:
        return f.read()


def get_install_requires() -> List[str]:

    return ["sqlalchemy"]


def get_tests_require() -> List[str]:

    return get_extras_require()["testing"]


def get_extras_require() -> Dict[str, List[str]]:

    requirements = {
        # "checking": ["black", "hacking", "mypy"],
        # "codecov": ["codecov", "pytest-cov"],
        # "doctest": [],
        # "document": ["sphinx", "sphinx_rtd_theme"],
        # "example": [],
        "testing": ["pytest", "pytest-dependency"],
    }

    return requirements


setup(
    name="optjournal",
    version=get_version(),
    description="Yet another Optuna RDB storage using journaling technique.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Takeru Ohta",
    author_email="phjgt308@gmail.com",
    url="https://github.com/sile/optjournal",
    packages=find_packages(),
    install_requires=get_install_requires(),
    tests_require=get_tests_require(),
    extras_require=get_extras_require(),
)
