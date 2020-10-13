from setuptools import setup, find_packages

with open("README.rst", "r") as fh:
  long_description = fh.read()

setup(
  name='elasticsearch-tocsv',
  version='0.0.8',
  author='Fabio Pipitone',
  author_email='fabio.pipitone93@gmail.com',
  url='https://github.com/fabiopipitone/elasticsearch-tocsv',
  description='Simple python tool to extract massive amounts of documents from Elasticsearch into a csv file exploiting multiprocessing and leveraging the underneath elasticsearch-py package',
  long_description=long_description,
  long_description_content_type='text/x-rst',
  license ='GPLv2',
  packages=find_packages(),
  entry_points={ 
    'console_scripts': [
      'elasticsearch_tocsv = elasticsearch_tocsv.elasticsearch_tocsv:main',
    ]
  },
  classifiers = [ 
    "Programming Language :: Python :: 3", 
    "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)", 
    "Operating System :: OS Independent", 
  ],
  keywords='elasticsearch, export, csv, report, kibana, elasticsearch-py',
  install_requires=['requests>==2.22.0', 'elasticsearch>=7.9.1', 'tqdm>=4.49.0', 'pandas>=1.1.2', 'pytz>=2020.1'],
  python_requires='>=3.8, <4',
  zip_safe = False
)