from setuptools import find_packages, setup


setup(
    name="dure",
    version="0.3.28",
    description="Resource-aware community LLM node bootstrapper",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.10",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=[],
    extras_require={
        "server": [
            "alembic>=1.13,<2",
            "fastapi>=0.110,<1",
            "psycopg[binary]>=3.1,<4",
            "sqlalchemy>=2.0,<3",
            "uvicorn>=0.29,<1",
        ],
        "test": [
            "alembic>=1.13,<2",
            "fastapi>=0.110,<1",
            "httpx>=0.27,<1",
            "psycopg[binary]>=3.1,<4",
            "sqlalchemy>=2.0,<3",
            "uvicorn>=0.29,<1",
        ],
    },
    entry_points={
        "console_scripts": [
            "dure=dure.cli:main",
            "dure-agent=dure.agent:main",
            "dure-server=dure.server:main",
        ]
    },
)
