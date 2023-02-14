from setuptools import setup, find_packages

setup(
    name='my-dataflow-job',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'googlemaps'
    ]
)