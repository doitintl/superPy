#!/bin/sh

python3 setup.py sdist
twine upload dist/*

