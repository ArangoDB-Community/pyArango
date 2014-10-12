from setuptools import setup, find_packages 
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='arangocity',

    version='1.0.0',

    description='An easy to use python driver for ArangoDB with built-in validation',
    long_description=long_description,
    
    url='https://github.com/tariqdaouda/arangocity',

    author='Tariq Daouda',
    author_email='tariq.daouda@umontreal.ca',

    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development :: Libraries',
        'Topic :: Database',
		'Topic :: Database :: Database Engines/Servers',
		'Topic :: Database :: Front-Ends',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2.7',
    ],

    keywords='database ORM sqlite3',

    packages=find_packages(),

    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)