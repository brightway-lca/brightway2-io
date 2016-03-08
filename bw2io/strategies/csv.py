# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *


is_empty_line = lambda line: not line or not any(line)


def csv_reformat(data):
    """Turn raw CSV data into list of activities and exchanges"""
    def get_exchanges(chunk, index):
        data = []
        columns = chunk[index + 1]
        for line in chunk[index + 2:]:
            if is_empty_line(line):
                continue
            data.append({key: value
                         for key, value in zip(columns, line)
                         if key})
        return data

    activity_indices = [i
                        for i, x in enumerate(data)
                        if not is_empty_line(x)
                        and x[0] == 'Activity']

    chunked = [data[i:j] for i, j in zip(activity_indices,
                                         activity_indices[1:] + [None])]
    reformatted = []

    for chunk in chunked:
        for index, line in enumerate(chunk):
            if is_empty_line(line):
                continue
            elif line[0] == 'Activity':
                assert line[1], "Can't understand activity name"
                chunk_data = {'name': line[1]}
            elif line[0] == 'Exchanges':
                chunk_data['exchanges'] = get_exchanges(chunk, index)
                break
            else:
                chunk_data[line[0]] = line[1]
        reformatted.append(chunk_data)

    return reformatted


def csv_restore_tuples(data):
    """Restore tuples separated by `::` string"""
    _ = lambda x: tuple(x.split("::")) if '::' in x else x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
            if key == 'categories' and isinstance(ds[key], str):
                ds[key] = (ds[key],)
        for exc in ds.get('exchanges', []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
                if key == 'categories' and isinstance(exc[key], str):
                    exc[key] = (exc[key],)
    return data


def csv_restore_booleans(data):
    """Turn `True` and `False` into proper booleans, where possible"""
    def _(x):
        if x.lower() == 'true':
            return True
        elif x.lower() == 'false':
            return False
        else:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get('exchanges', []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_numerize(data):
    """Turns strings into numbers where possible"""
    def _(x):
        try:
            return float(x)
        except:
            return x

    for ds in data:
        for key, value in ds.items():
            if isinstance(value, str):
                ds[key] = _(value)
        for exc in ds.get('exchanges', []):
            for key, value in exc.items():
                if isinstance(value, str):
                    exc[key] = _(value)
    return data


def csv_drop_unknown(data):
    """Drop keys whose values are `(Unknown)`."""
    _ = lambda x: None if x == "(Unknown)" else x

    data = [{k: v for k, v in ds.items()
             if v != '(Unknown)'}
            for ds in data]

    for ds in data:
        ds['exchanges'] = [{k: v for k, v in exc.items()
                            if v != '(Unknown)'}
                           for exc in ds.get('exchanges', [])]

    return data
