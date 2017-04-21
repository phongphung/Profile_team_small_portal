__author__ = 'sunary'


import os
from setuptools import setup, find_packages


def __path(filename):
    return os.path.join(os.path.dirname(__file__), filename)

build = 0
if os.path.exists(__path('build.info')):
    build = open(__path('build.info')).read().strip()

version = '1.1.0.{}'.format(build)

setup(
    name='sunary-tool',
    version=version,
    description='Tools',
    long_description=open('README.md').read(),
    author='Sunary',
    author_email='v2nhat@gmail.com',
    packages=find_packages(exclude=['docs', 'tests*']),
    install_requires=[],
    entry_points={
        'console_scripts': ['generate_processor = microservice.builder.generate_processor:main',
                            'generate_setting = microservice.builder.generate_setting:main']
    },
)