from setuptools import setup

setup(
    name='patek',
    version = '0.1',
    author = 'Khari Gardner',
    author_email = 'khgardner@proton.me',
    readme='README.md',
    description='A collection of utilities and tools for accelerating pyspark development and productivity.',
    url='https://github.com/kharigardner/Patek',
    packages=['patek'],
    install_requires=['pyspark', 'delta-spark'])
