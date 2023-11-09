# importing csv module


import csv

import time

import itertools

import json

from pathlib import Path

import shutil


def multigen(gen_func):
    class _multigen(object):

        def __init__(self, *args, **kwargs):
            self.__args = args

            self.__kwargs = kwargs

        def __iter__(self):
            return gen_func(*self.__args, **self.__kwargs)

    return _multigen
