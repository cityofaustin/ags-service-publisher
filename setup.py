from distutils.core import setup

setup(
    name='ags_service_publisher',
    version='1.0.0',
    packages=['ags_service_publisher'],
    install_requires=[
        'requests',
        'PyYAML'
    ],
    url='https://github.austintexas.gov/pughl/ags-service-publisher',
    license='',
    author='Logan Pugh',
    author_email='logan.pugh@austintexas.gov',
    description='Tools for publishing and managing services on ArcGIS Server'
)
