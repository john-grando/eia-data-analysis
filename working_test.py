#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 23 08:20:59 2020

@author: grandocu
"""

import pandas as pd

df_a = pd.DataFrame.from_dict({
    'a': [1,2,3,4,5,65],
    'b': [5,4,2,23,2,2]})


df = pd.DataFrame.from_dict({
    'a': [1,2,3,4,5,65],
    'b': [5,8,2,88,2,2],
    'c': ['o', 'k', 'k', 'o', 'k', 'd']})

df_2 = df.pivot(
    columns = ['c'],
    values = 'b'
    )

print(df_2)
print(df_2.stack('c'))

print(df.loc[df['a'].astype(str) == '2'])

print(df.merge(df_a, how="inner", left_index=True, right_index=True))
