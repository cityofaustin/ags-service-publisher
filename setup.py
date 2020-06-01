from distutils.core import setup

setup(
    name='ags_service_publisher',
    version='1.4.2',
    packages=['ags_service_publisher', 'ags_service_publisher.reporters'],
    install_requires=[
        'requests',
        'PyYAML'
    ],
    url='https://github.com/cityofaustin/ags-service-publisher',
    license='CC0',
    author='Logan Pugh',
    author_email='logan.pugh@austintexas.gov',
    description='Tools for publishing and managing services on ArcGIS Server'
)
