import os

from setuptools import setup, find_packages

path = os.path.abspath(os.path.dirname(__file__))


def read(filename):
    with open(os.path.join(path, filename), encoding='utf-8') as f:
        return f.read()


setup(
    name='s5a',
    version='0.1',
    description='Sentinel-5 Algorithms',
    author='Emissions API Developers',
    license='MIT',
    url='https://github.com/emissions-api/sentinel5algorithms',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering :: GIS',
    ],
    install_requires=read('requirements.txt').split(),
    long_description=read('README.rst'),
    long_description_content_type='text/x-rst',
)
