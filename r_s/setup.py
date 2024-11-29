from setuptools import find_packages, setup

setup(
    name="r_s",
    packages=find_packages(exclude=["r_s_tests"]),
    install_requires=[
        "dagster",
        "dagster-mlflow",
        "mlflow",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
