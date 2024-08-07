from setuptools import setup

setup(
    setup_requires=[
        "setuptools_scm",
    ],
    install_requires=[
        'requests',
        'PyYAML',
    ],
    packages=[
        'ags_service_publisher',
        'ags_service_publisher.reporters',
    ],
    include_package_data=True,
    use_scm_version=True,
)
