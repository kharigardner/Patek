from setuptools import setup

readme = open('README.md', 'r').read()

setup(
    name='patek',
    version = '0.5.1',
    author = 'Khari Gardner',
    author_email = 'khgardner@proton.me',
    readme='README.md',
    description='A collection of utilities and tools for accelerating pyspark development and productivity.',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/kharigardner/Patek',
    packages=['patek'],
    install_requires=['pyspark', 'delta-spark']
)
