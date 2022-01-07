from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    'google-cloud-bigquery==2.17.0',
    'apache-beam[gcp]==2.27.0',
    'google-api-core==1.28.0',
    'google-api-python-client==1.8.4'
]

setup(
    name='mf_data_ingestion_df',
    version='1.0',
    description='Job to put incremental data from AMFI to Bigquery',
    author='yashashri2102',
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages()
)
