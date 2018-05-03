# quark-fraud
fraud_processing_daily_report of quark
# -*- coding: utf-8 -*-

import os as os
import numpy as np
import pandas as pd
import math
from pandas import Series, DataFrame
import xlsxwriter as xw
from pandas.tseries.offsets import Day
from xlsxwriter.utility import xl_rowcol_to_cell
from datetime import datetime
from time import strptime, strftime
import datetime as dt
from datetime import datetime
import sys
reload(sys)
sys.setdefaultencoding('gbk') 

#路径
os.chdir(u'D:/数据源/每日/fraud')


