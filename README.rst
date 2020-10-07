Python Elasticsearch-to-CSV Export Tool
=======================================

Simple Python tool to easily extract a massive amount of Elasticsearch documents into a csv file, exploiting multiprocessing features and leveraging the underneath elasticsearch-py package.

Requirements
------------

In order to install and use the elasticsearch-tocsv tool, you need to install the following packages first:

  * `elasticsearch-py <https://pypi.org/project/elasticsearch>`_
  * `requests <https://pypi.org/project/requests/>`_
  * `pandas <https://pypi.org/project/pandas/>`_
  * `pytz <https://pypi.org/project/pytz/>`_
  * `tqdm <https://pypi.org/project/tqdm/>`_ (unless you want to disable the loading progress bars)


Installation
------------

Install the ``elasticsearch-tocsv`` package with:

    $ pip3 install elasticsearch-tocsv
