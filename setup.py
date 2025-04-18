from setuptools import find_packages, setup

setup(
    name="nba_etl",
    packages=find_packages(where="src",exclude=["nba_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
