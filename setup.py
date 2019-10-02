from setuptools import setup

setup(
    setup_requires=[
        "setuptools_scm",
    ],
    install_requires=[
        'requests',
        'PyYAML',
    ],
    use_scm_version=True,
)
