import steamspypi
import json
import csv
import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import math
from textwrap import dedent
import traceback


def merge_rates_availability():
    df_r = pd.read_csv("carpark_rates_cleaned.csv")
    df_a = pd.read_csv("carpark_availability_cleaned.csv")
    df_m = pd.read_csv("carpark_master_cleaned.csv")
    df_r.rename({'lotType':'lotTypeR'},axis=1,inplace=True)
    df_temp = df_a.merge(df_r,how='right',left_on=['carparkNo','lotType'],
                         right_on=['carparkNo','lotTypeR'])
    df_temp = df_temp.merge(df_m,how='left',left_on=['carparkNo'],right_on
                            =['carparkNo'])
    df_temp.to_csv("merged_carparks.csv")

merge_rates_availability()
                
