"""Setup configuration for the batch processing system."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README for the long description
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="batch-processing-system",
    version="1.0.0",
    author="Media Blackout LLC",
    author_email="contact@mediablackout.ai",
    description="A fault-tolerant batch processing pipeline integrating AWS DynamoDB with OpenAI Batch API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/MediaBlackout/batch_public",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.34.0,<2.0.0",
        "openai>=1.12.0,<2.0.0",
        "python-dotenv>=1.0.0,<2.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "moto>=5.0.0",
            "responses>=0.25.0",
            "black>=24.0.0",
            "ruff>=0.3.0",
            "mypy>=1.8.0",
            "boto3-stubs[dynamodb]>=1.34.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "batch-process=batch.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "batch": ["templates/*.py", "templates/README.md"],
    },
)
