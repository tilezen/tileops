# Python Environment Setup

The instructions below will assume Amazon Linux, but similar packages should exist and work on Ubuntu.

1. The Python pieces of this codebase require Python 3. Make sure your environment has a Python 3 version installed:

       sudo yum install python3

1. Install Pipenv

       pip3 install --user pipenv

1. While in the `tileops` directory, create a Pipenv environment:

       pipenv install

1. Use Pipenv to enter a shell in the virtual environment:

       pipenv shell

