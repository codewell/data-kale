from setuptools import setup

setup(
    setup_requires=['pbr', 'setuptools_scm', 'pytest-runner'],
    pbr=True,
    use_scm_version=True,
)
