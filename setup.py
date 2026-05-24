from setuptools import setup, find_packages

setup(
    name="negarabank-pipeline",
    version="0.1.0",
    author="NegaraBank Team",
    description="Production-grade data platform for transaction processing and fraud detection",
    url="https://github.com/negarabank/pipeline",
    packages=find_packages(),
    install_requires=[
        "databricks-sdk==0.20.0",
        "pyspark==3.5.0",
        "delta-spark==3.1.0",
        "boto3==1.28.0",
        "kafka-python==2.0.2",
        "mlflow==2.10.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.3",
            "pytest-cov==4.1.0",
            "black==23.12.0",
            "flake8==6.1.0",
        ]
    },
)
