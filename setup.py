from setuptools import setup

setup(
    name='patek',
    version = '0.3',
    author = 'Khari Gardner',
    author_email = 'khgardner@proton.me',
    readme='README.md',
    description='A collection of utilities and tools for accelerating pyspark development and productivity.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/kharigardner/Patek',
    packages=['patek'],
    install_requires=['pyspark', 'delta-spark']
)
