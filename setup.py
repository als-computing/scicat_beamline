from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))


def read_requirements(filename):
    with open(path.join(here, filename)) as requirements_file:
        # Parse requirements.txt, ignoring any commented-out lines.
        requirements = [
            line
            for line in requirements_file.read().splitlines()
            if not line.startswith("#")
        ]
    return requirements


setup(
    name="Scicat Dataset",
    version="1.0.0",
    url="https://github.com/mypackage.git",
    author="Author Name",
    author_email="author@gmail.com",
    description="Description of my package",
    install_requires=read_requirements("requirements.txt"),
    packages=find_packages(),
)
