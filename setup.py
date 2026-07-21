from setuptools import find_packages, setup

setup(
    name="saem",
    version="0.1.0",
    description="Saem RAG cluster: install-once, role-assign-from-head node package",
    packages=find_packages(exclude=["*.egg-info", "build", "dist"]),
    python_requires=">=3.9",
    install_requires=[
        "fastapi",
        "uvicorn",
        "httpx",
        "click",
        "pyyaml",
        "qdrant-client",
        # fastembed (onnxruntime), not sentence-transformers — the latter
        # drags in torch, which is multiple GB these 3GB VMs don't need.
        "fastembed",
        "ddgs",
        "trafilatura",
        # trafilatura imports this lazily; without it the crawler role fails
        # to start at all.
        "lxml_html_clean",
    ],
    entry_points={"console_scripts": ["saem=saem.cli:main"]},
)
