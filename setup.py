#!/usr/bin/env python3
"""
Setup script for the DBChecker SQLite Database Comparator.
"""

from setuptools import setup, find_packages
import os

# Read README file
readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
if os.path.exists(readme_path):
    with open(readme_path, 'r', encoding='utf-8') as f:
        long_description = f.read()
else:
    long_description = "SQLite Database Comparator - Compare two SQLite databases for structural and data equality while ignoring UUID differences"

setup(
    name="dbchecker",
    version="1.0.0",
    author="DBChecker Team",
    author_email="admin@dbchecker.com",
    description="SQLite Database Comparator for structural and data equality checking",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/HomeAutobots/dbchecker",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Testing",
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
        # Core dependencies are built-in to Python
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0", 
            "pytest-mock>=3.10.0",
        ],
        "reporting": [
            "jinja2>=3.1.0",
            "tabulate>=0.9.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "dbchecker=dbchecker.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
