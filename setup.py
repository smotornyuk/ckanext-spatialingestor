from setuptools import setup, find_packages

version = '0.1'

setup(
    name='ckanext-spatialingestor',
    version=version,
    description='CKAN Extension - Spatial Ingestor',
    long_description='Extension for interfacing with the CKAN spatial ingestor microservice',
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Greg von Nessi',
    author_email='greg.vonnessi@linkdigital.com.au',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.spatialingestor'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points="""

        [ckan.plugins]
        spatialingestor=ckanext.spatialingestor.plugin:SpatialIngestorPlugin

        [paste.paster_command]
        spatialingestor=ckanext.spatialingestor.cli:SpatialIngestorCommand
    """,
)
