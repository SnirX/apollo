from setuptools import setup, find_packages

setup(
    name='apollo_cli',
    description='Tool for snapshot/restore Cassandra clusters to S3 buckets',
    long_description='Apollo allows you to take full/incremental snapshots of your Cassandra cluster and restore them',
    version='0.1.2',
    license='GPLv3+',
    author='Snir Nissan',
    author_email='snir994@gmail.com',
    url='https://github.com/SnirX/apollo',
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': ['apollo=apollo_cli.apollo:cli'],
    },
    install_requires=[
        'Click>=7.0',
        'boto3>=1.9.0'
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: Utilities',
    ]
)

