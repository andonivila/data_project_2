# Setup file to install dependences on dataflow workers 


from setuptools import setup, find_packages

setup(
    name='dataflowjobtest',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'googlemaps',
        'python-dotenv'
    ]
)