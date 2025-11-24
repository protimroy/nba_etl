from setuptools import find_packages, setup

setup(
    name="nba_etl",
    version="0.1.0",                     # it’s good to pin a version
    package_dir={"": "src"},             # ← point “root” to src/
    packages=find_packages(
        where="src",
        exclude=["nba_etl_tests"]
    ),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "nba_api",
        "openai",
        "superduper",
        "pandas",
        "python-dotenv",
        "superduper-framework",
        "requests",
        "psycopg2-binary",
        "pymongo",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
    },
)