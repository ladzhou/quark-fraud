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


           
RawData_all = pd.read_csv('fraud.csv', encoding = 'gbk')
RawData_all.columns = ['ID', 'appdate', 'decdate', 'status', 'des', 'usere_eo', 'score', 'usera_tf', 'userc_tf', 'userb_tf', 'userb_ta'
                      , 'userb_tc', 'userc', 'userb', 'userb_city', 'fraud', 'fraud_city', 'fraud_result', 'usera_back', 'userc_back'
                      , 'sa_staff', 'sa_date', 'sa_city', 'sa_risk', 'su_staff', 'su_date', 'su_city', 'su_risk']
                

def styleContent(wb,**kwgs):    
    format = wb.add_format({'bold':False,'font_name':u'微软雅黑','font_size':10,'font_color':'black', 'align': 'center', 'valign': 'vcenter'})
    try:format.set_top(kwgs['top_border'])
    except KeyError: format.set_top(0)
    try:format.set_left(kwgs['left_border'])
    except KeyError: format.set_left(0)
    try:format.set_right(kwgs['right_border'])
    except KeyError: format.set_right(0)
    try:format.set_bottom(kwgs['bottom_border'])
    except KeyError: format.set_bottom(0)
    try:format.set_num_format(kwgs['num_format'])
    except KeyError: format.set_num_format('0')
    try:format.set_align(kwgs['h_align'])
    except KeyError:format.set_align('center')
    try:format.set_align(kwgs['v_align'])
    except KeyError:format.set_align('vcenter')
    try:
        bold = kwgs['bold']
        format.set_bold(bold)
    except KeyError: pass
    try: format.set_font_name(kwgs['font_name'])
    except KeyError: pass
    try: format.set_font_size(kwgs['font_size'])
    except KeyError: wb.add_format({'font_size':11})
    try:format.set_font_color(kwgs['font_color'])
    except KeyError: pass
    try:
        shrink = kwgs['shrink']
        format.set_shrink(shrink)
    except KeyError: pass
    try:format.set_bg_color(kwgs['bg_color'])
    except KeyError: pass
    try:format.set_fg_color(kwgs['fg_color'])
    except KeyError: pass      
#    format.set_text_wrap()
    return format
    
def division_calculator(fenzi_list, fenmu_list):
    per_list = []
    for i, fl in enumerate(fenmu_list):
        if fl == 0:
            per_list.append('-')
        else:
            per_list.append(float(fenzi_list[i]) / fl)
    return per_list

def get_count_type_list(RawData_all, column_name, time_list):
    unit_dataframe = RawData_all[RawData_all[column_name].notnull()]
    cnt_list = []
    for t, tl in enumerate(time_list):
        for i, td in enumerate(tl):
            if t == 0:
                cnt_frame = unit_dataframe
            else:
                cnt_frame = unit_dataframe[unit_dataframe[column_name].str.contains(td)] 
            cnt_list.append(cnt_frame['ID'].nunique())
    return cnt_list

def write_sheet_title(wb, ws, year_sort_list, month_sort_list, month_date_list):
    ws.merge_range(1, 1, 1, 3, u'日期', styleContent(wb, **{'bold': True, 'left_border': 2, 'top_border': 2}))
    ws.write(1, 4, 'TOTAL', styleContent(wb, **{'bold': True, 'left_border': 1, 'top_border': 2}))
    for y, ysl in enumerate(year_sort_list):
        ws.write(1, 5 + y, 'Y' + ysl, styleContent(wb, **{'bold': True, 'left_border': 1, 'top_border': 2}))
    for m, msl in enumerate(month_sort_list):
        ws.write(1, 5 + len(year_sort_list) + m, 'M' + msl[5:], styleContent(wb, **{'bold': True, 'left_border': 1, 'top_border': 2}))
    for d, mdl in enumerate(month_date_list):
        ws.write(1, 5 + len(year_sort_list) + len(month_sort_list) + d, mdl[5:], styleContent(wb, **{'bold': True, 'left_border': 1, 'top_border': 2}))

def select_dataframe(RawData_all, city_column):
    unit_dataframe = RawData_all[RawData_all[city_column].notnull()]
    return unit_dataframe
    
def generate_excel_data(unit_dataframe, time_list):
    #进件量
    app_cnt_list = get_count_type_list(unit_dataframe, 'appdate', time_list)
    #审批处理量
    dec_cnt_list = get_count_type_list(unit_dataframe, 'decdate', time_list)
    #规则引擎报警量
    sys_riskdata = unit_dataframe[(unit_dataframe['score'] >= 130) & (unit_dataframe['userb'].notnull())]
    sysrisk_cnt_list = get_count_type_list(select_dataframe(sys_riskdata, 'userb_city'), 'usere_eo', time_list)
    #规则引擎处理量（确认欺诈、退回初审、提交终审）
    sys_fraud_cnt_list = get_count_type_list(select_dataframe(sys_riskdata, 'userb_city'), 'userb_tf', time_list)
    sys_back_cnt_list = get_count_type_list(select_dataframe(sys_riskdata, 'userb_city'), 'userb_ta', time_list)
    sys_fback_cnt_list = get_count_type_list(select_dataframe(sys_riskdata, 'userb_city'), 'userb_tc', time_list)
    #审批提报反欺诈量
    usera_tf_cnt_list = get_count_type_list(select_dataframe(unit_dataframe, 'fraud_city'), u'usera_tf', time_list)
    userc_tf_cnt_list = get_count_type_list(select_dataframe(unit_dataframe, 'fraud_city'), u'userc_tf', time_list)
    #审批提报处理量（确认欺诈、退回初审、退回终审）
    credit_data = unit_dataframe[(unit_dataframe['userb_tf'].isnull()) & ((unit_dataframe['fraud_result'] == u'初审黑名单确认') | (unit_dataframe['fraud_result'] == u'终审黑名单确认'))]
    credit_fraud_cnt_list = get_count_type_list(select_dataframe(credit_data, 'fraud_city'), u'decdate', time_list)
    
    usera_back_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['fraud_result'] == u'初审黑名单退回'], 'fraud_city'), 'usera_back', time_list)
    userc_back_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['fraud_result'] == u'终审黑名单退回'], 'fraud_city'), 'userc_back', time_list)
  
    #城市抽查
    sample_cnt_list = get_count_type_list(select_dataframe(unit_dataframe, 'sa_city'), 'sa_date', time_list)
    sample_risk_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['sa_risk'] == 'Y'], 'sa_city'), 'sa_date', time_list)
    sample_norisk_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['sa_risk'] == 'N'], 'sa_city'), 'sa_date', time_list)
    #城市调查
    survey_cnt_list = get_count_type_list(select_dataframe(unit_dataframe, 'su_city'), 'su_date', time_list)
    survey_risk_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['su_risk'] == 'Y'], 'su_city'), 'su_date', time_list)
    survey_norisk_cnt_list = get_count_type_list(select_dataframe(unit_dataframe[unit_dataframe['su_risk'] == 'N'], 'su_city'), 'su_date', time_list)
    
    #欺诈总量
    total_fraud_data = unit_dataframe[(unit_dataframe['status'] == u'反欺诈确认') & (unit_dataframe['des'].notnull())]
    total_fraud_cnt_list = get_count_type_list(select_dataframe(total_fraud_data, 'fraud_city'), 'decdate', time_list)
    #黑灰名单
    black_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（黑）')]
    black_cnt_list = get_count_type_list(select_dataframe(black_data, 'fraud_city'), u'decdate', time_list)
    
    gray_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（灰）')]
    gray_cnt_list = get_count_type_list(select_dataframe(gray_data, 'fraud_city'), 'decdate', time_list)

    #规则引擎处理量
    sys_cnt_list = map(lambda(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list): sys_fraud_cnt_list + sys_back_cnt_list + sys_fback_cnt_list, zip(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list))
    #审批提报处理量
    credit_cnt_list = map(lambda(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list): credit_fraud_cnt_list + usera_back_cnt_list + userc_back_cnt_list, zip(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list))
    #总处理量
    fraud_dec_cnt_list = map(lambda(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list): sys_cnt_list + credit_cnt_list + sample_cnt_list + survey_cnt_list, zip(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list))
    
    #总欺诈率=（规则引擎处理量确认欺诈+审批提报处理量确认欺诈+城市抽查确认欺诈风险+城市调查确认欺诈风险）/反欺诈总处理量
    fraud_cnt_list = map(lambda(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list): sys_fraud_cnt_list + credit_fraud_cnt_list + sample_risk_cnt_list + survey_risk_cnt_list, zip(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list))
    fraud_per_list = division_calculator(fraud_cnt_list, fraud_dec_cnt_list)   
    #规则引擎欺诈率=规则引擎处理量确认欺诈/规则引擎处理量
    sys_fraud_per_list = division_calculator(sys_fraud_cnt_list, sys_cnt_list) 
    #审批提报欺诈率=审批提报处理量确认欺诈/审批提报处理量
    credit_fraud_per_list = division_calculator(credit_fraud_cnt_list, credit_cnt_list)
    #城市抽查欺诈率=城市抽查确认欺诈风险量/城市抽查量
    sa_fraud_per_list = division_calculator(sample_risk_cnt_list, sample_cnt_list)
    #城市调查欺诈率
    su_fraud_per_list = division_calculator(survey_risk_cnt_list, survey_cnt_list)
    #规则引擎回退率=规则引擎处理量退回初审/规则引擎处理量
    sys_back_per_list = division_calculator(sys_back_cnt_list, sys_cnt_list)
    #审批提报回退率=（审批提报处理量退回初审+审批提报处理量退回终审）/审批提报处理量
    credit_back_cnt_list = map(lambda(usera_back_cnt_list, userc_back_cnt_list): usera_back_cnt_list + userc_back_cnt_list, zip(usera_back_cnt_list, userc_back_cnt_list))
    credit_back_per_list = division_calculator(credit_back_cnt_list, credit_cnt_list)
    
    
    dbbz_data = total_fraud_data[total_fraud_data['des'] == u'代办包装（黑）']
    dbbz_cnt_list = get_count_type_list(select_dataframe(dbbz_data, 'fraud_city'), 'decdate', time_list)
    
    wmsq_data = total_fraud_data[total_fraud_data['des'] == u'伪冒申请（黑）']
    wmsq_cnt_list = get_count_type_list(select_dataframe(wmsq_data, 'fraud_city'), u'decdate', time_list)
    
    ztpd_data = total_fraud_data[total_fraud_data['des'] == u'组团骗贷（黑）']
    ztpd_cnt_list = get_count_type_list(select_dataframe(ztpd_data, 'fraud_city'), 'decdate', time_list)
    
    zlxj_data = total_fraud_data[total_fraud_data['des'] == u'资料虚假（黑）']
    zlxj_cnt_list = get_count_type_list(select_dataframe(zlxj_data, 'fraud_city'), 'decdate', time_list)
    
    zyxj_data = total_fraud_data[total_fraud_data['des'] == u'职业信息虚假（灰）']
    zyxj_cnt_list = get_count_type_list(select_dataframe(zyxj_data, 'fraud_city'), 'decdate', time_list)
    
    lzsq_data = total_fraud_data[total_fraud_data['des'] == u'离职后申请（灰）']
    lzsq_cnt_list = get_count_type_list(select_dataframe(lzsq_data, 'fraud_city'), 'decdate', time_list)
    
    dwxj_data = total_fraud_data[total_fraud_data['des'] == u'虚假单位（灰）']
    dwxj_cnt_list = get_count_type_list(select_dataframe(dwxj_data, 'fraud_city'), 'decdate', time_list)
    
    blsh_data = total_fraud_data[total_fraud_data['des'] == u'不良嗜好（灰）']
    blsh_cnt_list = get_count_type_list(select_dataframe(blsh_data, 'fraud_city'), 'decdate', time_list)
    
    whknl_data = total_fraud_data[total_fraud_data['des'] == u'无还款能力（灰）']
    whknl_cnt_list = get_count_type_list(select_dataframe(whknl_data, 'fraud_city'), 'decdate', time_list)
    
    cqtq_data = total_fraud_data[total_fraud_data['des'] == u'长期拖欠（灰）']
    cqtq_cnt_list = get_count_type_list(select_dataframe(cqtq_data, 'fraud_city'), 'decdate', time_list)
    
    ytfx_data = total_fraud_data[total_fraud_data['des'] == u'用途风险（灰）']
    ytfx_cnt_list = get_count_type_list(select_dataframe(ytfx_data, 'fraud_city'), 'decdate', time_list)
    
    qt_data = total_fraud_data[total_fraud_data['des'] == u'其他（灰）']
    qt_cnt_list = get_count_type_list(select_dataframe(qt_data, 'fraud_city'), 'decdate', time_list)
    
    main_data_list = [app_cnt_list, dec_cnt_list, fraud_dec_cnt_list, sysrisk_cnt_list, sys_fraud_cnt_list, sys_back_cnt_list
                     , sys_fback_cnt_list, usera_tf_cnt_list, userc_tf_cnt_list, credit_fraud_cnt_list, usera_back_cnt_list
                     , userc_back_cnt_list, sample_risk_cnt_list, sample_norisk_cnt_list, survey_risk_cnt_list, survey_norisk_cnt_list
                     , total_fraud_cnt_list, black_cnt_list, gray_cnt_list, fraud_per_list, sys_fraud_per_list, credit_fraud_per_list
                     , sa_fraud_per_list, su_fraud_per_list, sys_back_per_list, credit_back_per_list, dbbz_cnt_list, wmsq_cnt_list
                     , ztpd_cnt_list, zlxj_cnt_list, zyxj_cnt_list, lzsq_cnt_list, dwxj_cnt_list, blsh_cnt_list, whknl_cnt_list
                     , cqtq_cnt_list, ytfx_cnt_list, qt_cnt_list]
                                                             
    return main_data_list
    

def write_into_excel(wb, ws, main_data_list):
    ws.merge_range(2, 2, 2, 3, prog_list[0], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(3, 2, 3, 3, prog_list[1], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(4, 2, 4, 3, prog_list[2], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(5, 2, 5, 3, prog_list[3], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(6, 2, 8, 2, u'规则引擎处理量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(6, 3, prog_list[4], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(7, 3, prog_list[5], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.write(8, 3, prog_list[6], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(9, 2, 10, 2, u'审批提报反欺诈量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(9, 3, prog_list[7], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(10, 3, prog_list[8], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(11, 2, 13, 2, u'审批提报处理量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(11, 3, prog_list[9], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(12, 3, prog_list[10], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.write(13, 3, prog_list[11], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(14, 2, 15, 2, u'城市抽查量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(14, 3, prog_list[12], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(15, 3, prog_list[13], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(16, 2, 17, 2, u'城市调查量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(16, 3, prog_list[12], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(17, 3, prog_list[13], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(18, 2, 18, 3, prog_list[14], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(19, 2, 19, 3, prog_list[15], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'h_align': 'right'}))
    ws.merge_range(20, 2, 20, 3, prog_list[16], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'h_align': 'right'}))
    ws.merge_range(21, 2, 21, 3, prog_list[17], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(22, 2, 22, 3, prog_list[18], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(23, 2, 23, 3, prog_list[19], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(24, 2, 24, 3, prog_list[20], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(25, 2, 25, 3, prog_list[21], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(26, 2, 26, 3, prog_list[22], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(27, 2, 27, 3, prog_list[23], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(2, 1, 27, 1, u'总计', styleContent(wb, **{'left_border': 2, 'top_border': 1}))
    ws.merge_range(28, 1, 28, 3, prog_list[24], styleContent(wb, **{'left_border': 2, 'top_border': 1}))
    ws.merge_range(29, 1, 29, 3, prog_list[25], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(30, 1, 30, 3, prog_list[26], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(31, 1, 31, 3, prog_list[27], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(32, 1, 32, 3, prog_list[28], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(33, 1, 33, 3, prog_list[29], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(34, 1, 34, 3, prog_list[30], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(35, 1, 35, 3, prog_list[31], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(36, 1, 36, 3, prog_list[32], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(37, 1, 37, 3, prog_list[33], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(38, 1, 38, 3, prog_list[34], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(39, 1, 39, 3, prog_list[35], styleContent(wb, **{'left_border': 2, 'top_border': 3, 'bottom_border': 2}))
    
    #填数据
    ws.write_row(2, 4, main_data_list[0][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(3, 4, main_data_list[1][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(4, 4, main_data_list[2][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(5, 4, main_data_list[3][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(6, 4, main_data_list[4][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(7, 4, main_data_list[5][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(8, 4, main_data_list[6][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(9, 4, main_data_list[7][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(10, 4, main_data_list[8][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(11, 4, main_data_list[9][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(12, 4, main_data_list[10][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(13, 4, main_data_list[11][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(14, 4, main_data_list[12][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(15, 4, main_data_list[13][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(16, 4, main_data_list[14][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(17, 4, main_data_list[15][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(18, 4, main_data_list[16][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(19, 4, main_data_list[17][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0', 'h_align': 'right'}))
    ws.write_row(20, 4, main_data_list[18][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0', 'h_align': 'right'}))
    ws.write_row(21, 4, main_data_list[19][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#0.00%'}))
    ws.write_row(22, 4, main_data_list[20][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(23, 4, main_data_list[21][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(24, 4, main_data_list[22][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(25, 4, main_data_list[23][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(26, 4, main_data_list[24][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(27, 4, main_data_list[25][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(28, 4, main_data_list[26][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(29, 4, main_data_list[27][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(30, 4, main_data_list[28][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(31, 4, main_data_list[29][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(32, 4, main_data_list[30][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(33, 4, main_data_list[31][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(34, 4, main_data_list[32][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(35, 4, main_data_list[33][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(36, 4, main_data_list[34][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(37, 4, main_data_list[35][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(38, 4, main_data_list[36][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(39, 4, main_data_list[37][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'bottom_border': 2, 'num_format': '#,###,##0'}))
    
    ws.write('B42', u'数据说明：', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B43', u'总欺诈率=（规则引擎处理量确认欺诈+审批提报处理量确认欺诈+城市抽查确认欺诈风险+城市调查确认欺诈风险）/反欺诈总处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B44', u'规则引擎欺诈率=规则引擎处理量确认欺诈/规则引擎处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B45', u'审批提报欺诈率=审批提报处理量确认欺诈/审批提报处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B46', u'城市抽查欺诈率=城市抽查确认欺诈风险量/城市抽查量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B47', u'城市调查欺诈率=城市调查确认欺诈风险量/城市调查量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B48', u'规则引擎回退率=规则引擎处理量退回初审/规则引擎处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B49', u'审批提报回退率=（审批提报处理量退回初审+审批提报处理量退回终审）/审批提报处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B50', u'反欺诈处理总量=规则引擎处理量+审批提报处理量+城市抽查量+城市调查量', styleContent(wb, **{'h_align': 'left'}))    
    
    for i in range(1, 40):
        ws.write(i, 5 + len(year_sort_list) + len(month_sort_list) + len(month_date_list), '', styleContent(wb, **{'left_border': 2}))
  
prog_list = [u'进件量', u'审批处理量', u'反欺诈处理量', u'规则引擎报警量', u'确认欺诈', u'退回初审', u'提交终审', u'初审', u'终审', u'确认欺诈'
             , u'退回初审', u'退回终审', u'欺诈风险', u'非欺诈风险', u'欺诈拒绝分类', u'黑名单', u'灰名单', u'总欺诈率', u'规则引擎欺诈率'
             , u'审批提报欺诈率', u'城市抽查欺诈率', u'城市调查欺诈率', u'规则引擎回退率', u'审批提报回退率', u'代办包装  (D0102)'
             , u'伪冒申请  (D0101)', u'组团骗贷  (D0103)', u'资料虚假  (D0104)', u'职业信息虚假  (D0201)', u'离职后申请  (D0204)'            
             , u'单位虚假  (D0202)', u'不良嗜好  (D0502)', u'无还款能力  (D0501)', u'长期拖欠 (D0505)', u'用途风险  (D0503)', u'其他  (D0504)']


now = datetime.now()
tod = datetime(now.year, now.month, now.day).date()
tod_str = tod.strftime('%Y%m%d')
yesterday = tod - dt.timedelta(days = 1)
yesterday_str = yesterday.strftime('%Y-%m-%d')
month_first_day = datetime(yesterday.year, yesterday.month, 1).date()
month_first_day_str = month_first_day.strftime('%Y-%m-%d')
year_first_month = datetime(yesterday.year, 1, 1).date()
year_first_month_str = year_first_month.strftime('%Y-%m-%d')

date_list1 = list(RawData_all[RawData_all['appdate'].notnull()]['appdate'].unique())
date_list2 = list(RawData_all[RawData_all['decdate'].notnull()]['decdate'].unique())
date_list3 = list(RawData_all[RawData_all['usere_eo'].notnull()]['usere_eo'].unique())

date_list = []
date_list.extend(date_list1)
date_list.extend(date_list2)
date_list.extend(date_list3)
date_sort_list = list(set(date_list))
date_sort_list.sort()

year_list = []
month_list = []
month_date_list = []


for d, dsl in enumerate(date_sort_list):
    year_list.append(dsl[:4])
    year_sort_list = list(set(year_list))
    year_sort_list.sort()
    if dsl >= year_first_month_str and dsl <= yesterday_str:
        month_list.append(dsl[:7])
        month_sort_list = list(set(month_list))
        month_sort_list.sort()        
    if dsl >= month_first_day_str and dsl <= yesterday_str:
        month_date_list.append(dsl)
        month_date_list.sort(reverse = True)

                
time_list = [['ALL']]
time_list.append(year_sort_list)
time_list.append(month_sort_list)
time_list.append(month_date_list)

wb = xw.Workbook('fraud_processing_daily_report_'+tod_str+'.xlsx')

#城市汇总-系统
ws = wb.add_worksheet(u'反欺诈汇总')
ws.freeze_panes(2, 4)
ws.set_column(0, 0, 2)
ws.set_column(2, 2, 14)
            
write_sheet_title(wb, ws, year_sort_list, month_sort_list, month_date_list)
#print(time_list)
main_data_list = generate_excel_data(RawData_all, time_list)
#print(main_data_list)
write_into_excel(wb, ws, main_data_list)

#反欺诈汇总 
def select_dataframe(RawData_all, city_column, city_list):
    unit_dataframe = RawData_all[RawData_all[city_column] == city_list]
    return unit_dataframe
    
def city_get_count_type_list(RawData_all, city_column, city_list, column_name, time_list):
    unit_dataframe = select_dataframe(RawData_all, city_column, city_list)
    unit_dataframe = unit_dataframe[unit_dataframe[column_name].notnull()]
    cnt_list = []
    for t, tl in enumerate(time_list):
        for i, td in enumerate(tl):
            if t == 0:
                cnt_frame = unit_dataframe
            else:
                cnt_frame = unit_dataframe[unit_dataframe[column_name].str.contains(td)] 
            cnt_list.append(cnt_frame['ID'].nunique())
    return cnt_list
    
def generate_excel_data(unit_dataframe, city, time_list):
    #规则引擎报警量
    sys_riskdata = unit_dataframe[(unit_dataframe['score'] >= 130) & (unit_dataframe['userb'].notnull())]
    sysrisk_cnt_list = city_get_count_type_list(sys_riskdata, 'userb_city', city, 'usere_eo', time_list)
    #规则引擎处理量（确认欺诈、退回初审、提交终审）
    sys_fraud_cnt_list = city_get_count_type_list(sys_riskdata, 'userb_city', city, 'userb_tf', time_list)
    sys_back_cnt_list = city_get_count_type_list(sys_riskdata, 'userb_city', city, 'userb_ta', time_list)
    sys_fback_cnt_list = city_get_count_type_list(sys_riskdata, 'userb_city', city, 'userb_tc', time_list)
    #审批提报反欺诈量
    usera_tf_cnt_list = city_get_count_type_list(unit_dataframe, 'fraud_city', city, u'usera_tf', time_list)
    userc_tf_cnt_list = city_get_count_type_list(unit_dataframe, 'fraud_city', city, u'userc_tf', time_list)
    #审批提报处理量（确认欺诈、退回初审、退回终审）
    credit_data = unit_dataframe[(unit_dataframe['userb_tf'].isnull()) & ((unit_dataframe['fraud_result'] == u'初审黑名单确认') | (unit_dataframe['fraud_result'] == u'终审黑名单确认'))]
    credit_fraud_cnt_list = city_get_count_type_list(credit_data, 'fraud_city', city, u'decdate', time_list)
    
    usera_back_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['fraud_result'] == u'初审黑名单退回'], 'fraud_city', city, 'usera_back', time_list)
    userc_back_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['fraud_result'] == u'终审黑名单退回'], 'fraud_city', city, 'userc_back', time_list)
  
    #城市抽查
    sample_cnt_list = city_get_count_type_list(unit_dataframe, 'sa_city', city, 'sa_date', time_list)
    sample_risk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['sa_risk'] == 'Y'], 'sa_city', city, 'sa_date', time_list)
    sample_norisk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['sa_risk'] == 'N'], 'sa_city', city, 'sa_date', time_list)
    #城市调查
    survey_cnt_list = city_get_count_type_list(unit_dataframe, 'su_city', city, 'su_date', time_list)
    survey_risk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['su_risk'] == 'Y'], 'su_city', city, 'su_date', time_list)
    survey_norisk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['su_risk'] == 'N'], 'su_city', city, 'su_date', time_list)
    
    #欺诈总量
    total_fraud_data = unit_dataframe[(unit_dataframe['status'] == u'反欺诈确认') & (unit_dataframe['des'].notnull())]
    total_fraud_cnt_list = city_get_count_type_list(total_fraud_data, 'fraud_city', city, 'decdate', time_list)
    #黑灰名单
    black_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（黑）')]
    black_cnt_list = city_get_count_type_list(black_data, 'fraud_city', city, u'decdate', time_list)
    
    gray_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（灰）')]
    gray_cnt_list = city_get_count_type_list(gray_data, 'fraud_city', city, 'decdate', time_list)

    #规则引擎处理量
    sys_cnt_list = map(lambda(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list): sys_fraud_cnt_list + sys_back_cnt_list + sys_fback_cnt_list, zip(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list))
    #审批提报处理量
    credit_cnt_list = map(lambda(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list): credit_fraud_cnt_list + usera_back_cnt_list + userc_back_cnt_list, zip(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list))
    #总处理量
    fraud_dec_cnt_list = map(lambda(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list): sys_cnt_list + credit_cnt_list + sample_cnt_list + survey_cnt_list, zip(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list))
    
    #总欺诈率=（规则引擎处理量确认欺诈+审批提报处理量确认欺诈+城市抽查确认欺诈风险+城市调查确认欺诈风险）/反欺诈总处理量
    fraud_cnt_list = map(lambda(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list): sys_fraud_cnt_list + credit_fraud_cnt_list + sample_risk_cnt_list + survey_risk_cnt_list, zip(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list))
    fraud_per_list = division_calculator(fraud_cnt_list, fraud_dec_cnt_list)   
    #规则引擎欺诈率=规则引擎处理量确认欺诈/规则引擎处理量
    sys_fraud_per_list = division_calculator(sys_fraud_cnt_list, sys_cnt_list) 
    #审批提报欺诈率=审批提报处理量确认欺诈/审批提报处理量
    credit_fraud_per_list = division_calculator(credit_fraud_cnt_list, credit_cnt_list)
    #城市抽查欺诈率=城市抽查确认欺诈风险量/城市抽查量
    sa_fraud_per_list = division_calculator(sample_risk_cnt_list, sample_cnt_list)
    #城市调查欺诈率
    su_fraud_per_list = division_calculator(survey_risk_cnt_list, survey_cnt_list)
    #规则引擎回退率=规则引擎处理量退回初审/规则引擎处理量
    sys_back_per_list = division_calculator(sys_back_cnt_list, sys_cnt_list)
    #审批提报回退率=（审批提报处理量退回初审+审批提报处理量退回终审）/审批提报处理量
    credit_back_cnt_list = map(lambda(usera_back_cnt_list, userc_back_cnt_list): usera_back_cnt_list + userc_back_cnt_list, zip(usera_back_cnt_list, userc_back_cnt_list))
    credit_back_per_list = division_calculator(credit_back_cnt_list, credit_cnt_list)
    
    
    dbbz_data = total_fraud_data[total_fraud_data['des'] == u'代办包装（黑）']
    dbbz_cnt_list = city_get_count_type_list(dbbz_data, 'fraud_city', city, 'decdate', time_list)
    
    wmsq_data = total_fraud_data[total_fraud_data['des'] == u'伪冒申请（黑）']
    wmsq_cnt_list = city_get_count_type_list(wmsq_data, 'fraud_city', city, u'decdate', time_list)
    
    ztpd_data = total_fraud_data[total_fraud_data['des'] == u'组团骗贷（黑）']
    ztpd_cnt_list = city_get_count_type_list(ztpd_data, 'fraud_city', city, 'decdate', time_list)
    
    zlxj_data = total_fraud_data[total_fraud_data['des'] == u'资料虚假（黑）']
    zlxj_cnt_list = city_get_count_type_list(zlxj_data, 'fraud_city', city, 'decdate', time_list)
    
    zyxj_data = total_fraud_data[total_fraud_data['des'] == u'职业信息虚假（灰）']
    zyxj_cnt_list = city_get_count_type_list(zyxj_data, 'fraud_city', city, 'decdate', time_list)
    
    lzsq_data = total_fraud_data[total_fraud_data['des'] == u'离职后申请（灰）']
    lzsq_cnt_list = city_get_count_type_list(lzsq_data, 'fraud_city', city, 'decdate', time_list)
    
    dwxj_data = total_fraud_data[total_fraud_data['des'] == u'虚假单位（灰）']
    dwxj_cnt_list = city_get_count_type_list(dwxj_data, 'fraud_city', city, 'decdate', time_list)
    
    blsh_data = total_fraud_data[total_fraud_data['des'] == u'不良嗜好（灰）']
    blsh_cnt_list = city_get_count_type_list(blsh_data, 'fraud_city', city, 'decdate', time_list)
    
    whknl_data = total_fraud_data[total_fraud_data['des'] == u'无还款能力（灰）']
    whknl_cnt_list = city_get_count_type_list(whknl_data, 'fraud_city', city, 'decdate', time_list)
    
    cqtq_data = total_fraud_data[total_fraud_data['des'] == u'长期拖欠（灰）']
    cqtq_cnt_list = city_get_count_type_list(cqtq_data, 'fraud_city', city, 'decdate', time_list)
    
    ytfx_data = total_fraud_data[total_fraud_data['des'] == u'用途风险（灰）']
    ytfx_cnt_list = city_get_count_type_list(ytfx_data, 'fraud_city', city, 'decdate', time_list)
    
    qt_data = total_fraud_data[total_fraud_data['des'] == u'其他（灰）']
    qt_cnt_list = city_get_count_type_list(qt_data, 'fraud_city', city, 'decdate', time_list)
    
    main_data_list = [fraud_dec_cnt_list, sysrisk_cnt_list, sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list, usera_tf_cnt_list
                     , userc_tf_cnt_list, credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list, sample_risk_cnt_list
                     , sample_norisk_cnt_list, survey_risk_cnt_list, survey_norisk_cnt_list, total_fraud_cnt_list, black_cnt_list
                     , gray_cnt_list, fraud_per_list, sys_fraud_per_list, credit_fraud_per_list, sa_fraud_per_list, su_fraud_per_list
                     , sys_back_per_list, credit_back_per_list, dbbz_cnt_list, wmsq_cnt_list, ztpd_cnt_list, zlxj_cnt_list, zyxj_cnt_list
                     , lzsq_cnt_list, dwxj_cnt_list, blsh_cnt_list, whknl_cnt_list, cqtq_cnt_list, ytfx_cnt_list, qt_cnt_list]
                                                             
    return main_data_list        

def write_into_excel(wb, ws, main_data_list):
    ws.merge_range(2, 2, 2, 3, prog_list[0], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(3, 2, 3, 3, prog_list[1], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(4, 2, 6, 2, u'规则引擎处理量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(4, 3, prog_list[2], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(5, 3, prog_list[3], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.write(6, 3, prog_list[4], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(7, 2, 8, 2, u'审批提报反欺诈量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(7, 3, prog_list[5], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(8, 3, prog_list[6], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(9, 2, 11, 2, u'审批提报处理量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(9, 3, prog_list[7], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(10, 3, prog_list[8], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.write(11, 3, prog_list[9], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(12, 2, 13, 2, u'城市抽查量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(12, 3, prog_list[10], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(13, 3, prog_list[11], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(14, 2, 15, 2, u'城市调查量', styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(14, 3, prog_list[10], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.write(15, 3, prog_list[11], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(16, 2, 16, 3, prog_list[12], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(17, 2, 17, 3, prog_list[13], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'h_align': 'right'}))
    ws.merge_range(18, 2, 18, 3, prog_list[14], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'h_align': 'right'}))
    ws.merge_range(19, 2, 19, 3, prog_list[15], styleContent(wb, **{'left_border': 1, 'top_border': 1}))
    ws.merge_range(20, 2, 20, 3, prog_list[16], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(21, 2, 21, 3, prog_list[17], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(22, 2, 22, 3, prog_list[18], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(23, 2, 23, 3, prog_list[19], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(24, 2, 24, 3, prog_list[20], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(25, 2, 25, 3, prog_list[21], styleContent(wb, **{'left_border': 1, 'top_border': 3}))
    ws.merge_range(2, 1, 25, 1, u'总计', styleContent(wb, **{'left_border': 2, 'top_border': 1}))
    ws.merge_range(26, 1, 26, 3, prog_list[22], styleContent(wb, **{'left_border': 2, 'top_border': 1}))
    ws.merge_range(27, 1, 27, 3, prog_list[23], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(28, 1, 28, 3, prog_list[24], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(29, 1, 29, 3, prog_list[25], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(30, 1, 30, 3, prog_list[26], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(31, 1, 31, 3, prog_list[27], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(32, 1, 32, 3, prog_list[28], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(33, 1, 33, 3, prog_list[29], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(34, 1, 34, 3, prog_list[30], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(35, 1, 35, 3, prog_list[31], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(36, 1, 36, 3, prog_list[32], styleContent(wb, **{'left_border': 2, 'top_border': 3}))
    ws.merge_range(37, 1, 37, 3, prog_list[33], styleContent(wb, **{'left_border': 2, 'top_border': 3, 'bottom_border': 2}))
    
    #填数据
    ws.write_row(2, 4, main_data_list[0][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(3, 4, main_data_list[1][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(4, 4, main_data_list[2][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(5, 4, main_data_list[3][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(6, 4, main_data_list[4][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(7, 4, main_data_list[5][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(8, 4, main_data_list[6][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(9, 4, main_data_list[7][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(10, 4, main_data_list[8][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(11, 4, main_data_list[9][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(12, 4, main_data_list[10][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(13, 4, main_data_list[11][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(14, 4, main_data_list[12][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(15, 4, main_data_list[13][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(16, 4, main_data_list[14][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(17, 4, main_data_list[15][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0', 'h_align': 'right'}))
    ws.write_row(18, 4, main_data_list[16][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0', 'h_align': 'right'}))
    ws.write_row(19, 4, main_data_list[17][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#0.00%'}))
    ws.write_row(20, 4, main_data_list[18][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(21, 4, main_data_list[19][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(22, 4, main_data_list[20][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(23, 4, main_data_list[21][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(24, 4, main_data_list[22][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(25, 4, main_data_list[23][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#0.00%'}))
    ws.write_row(26, 4, main_data_list[24][0:], styleContent(wb, **{'left_border': 1, 'top_border': 1, 'num_format': '#,###,##0'}))
    ws.write_row(27, 4, main_data_list[25][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(28, 4, main_data_list[26][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(29, 4, main_data_list[27][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(30, 4, main_data_list[28][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(31, 4, main_data_list[29][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(32, 4, main_data_list[30][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(33, 4, main_data_list[31][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(34, 4, main_data_list[32][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(35, 4, main_data_list[33][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(36, 4, main_data_list[34][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'num_format': '#,###,##0'}))
    ws.write_row(37, 4, main_data_list[35][0:], styleContent(wb, **{'left_border': 1, 'top_border': 3, 'bottom_border': 2, 'num_format': '#,###,##0'}))
    
    ws.write('B40', u'数据说明：', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B41', u'总欺诈率=（规则引擎处理量确认欺诈+审批提报处理量确认欺诈+城市抽查确认欺诈风险+城市调查确认欺诈风险）/反欺诈总处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B42', u'规则引擎欺诈率=规则引擎处理量确认欺诈/规则引擎处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B43', u'审批提报欺诈率=审批提报处理量确认欺诈/审批提报处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B44', u'城市抽查欺诈率=城市抽查确认欺诈风险量/城市抽查量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B45', u'城市调查欺诈率=城市调查确认欺诈风险量/城市调查量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B46', u'规则引擎回退率=规则引擎处理量退回初审/规则引擎处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B47', u'审批提报回退率=（审批提报处理量退回初审+审批提报处理量退回终审）/审批提报处理量', styleContent(wb, **{'h_align': 'left'}))
    ws.write('B48', u'反欺诈处理总量=规则引擎处理量+审批提报处理量+城市抽查量+城市调查量', styleContent(wb, **{'h_align': 'left'}))    
    
    for i in range(1, 38):
        ws.write(i, 5 + len(year_sort_list) + len(month_sort_list) + len(month_date_list), '', styleContent(wb, **{'left_border': 2}))

   
    
prog_list = [u'总处理总量', u'规则引擎报警量', u'确认欺诈', u'退回初审', u'提交终审', u'初审', u'终审', u'确认欺诈', u'退回初审', u'退回终审'
             , u'欺诈风险', u'非欺诈风险', u'欺诈拒绝分类', u'黑名单', u'灰名单', u'总欺诈率', u'规则引擎欺诈率', u'审批提报欺诈率', u'城市抽查欺诈率'
             , u'城市调查欺诈率', u'规则引擎回退率', u'审批提报回退率', u'代办包装  (D0102)', u'伪冒申请  (D0101)', u'组团骗贷  (D0103)'
             , u'资料虚假  (D0104)', u'职业信息虚假  (D0201)', u'离职后申请  (D0204)', u'单位虚假  (D0202)', u'不良嗜好  (D0502)'            
             , u'无还款能力  (D0501)', u'长期拖欠 (D0505)', u'用途风险  (D0503)', u'其他  (D0504)']

date_list1 = list(RawData_all[RawData_all['decdate'].notnull()]['decdate'].unique())
date_list2 = list(RawData_all[RawData_all['userb'].notnull()]['usere_eo'].unique())

date_list = []
date_list.extend(date_list1)
date_list.extend(date_list2)
date_sort_list = list(set(date_list))
date_sort_list.sort()

year_list = []
month_list = []
month_date_list = []

for d, dsl in enumerate(date_sort_list):
    year_list.append(dsl[:4])
    year_sort_list = list(set(year_list))
    year_sort_list.sort()
    if dsl >= year_first_month_str and dsl <= yesterday_str:
        month_list.append(dsl[:7])
        month_sort_list = list(set(month_list))
        month_sort_list.sort()
    if dsl >= month_first_day_str and dsl <= yesterday_str:
        month_date_list.append(dsl)
        month_date_list.sort(reverse = True)
        
time_list = [['ALL']]
time_list.append(year_sort_list)
time_list.append(month_sort_list)
time_list.append(month_date_list)

city_list = [u'上海', u'武汉']
for c, city in enumerate(city_list):
    ws = wb.add_worksheet(city + u'-反欺诈汇总')
    
    ws.freeze_panes(2, 4)
    ws.set_column(0, 0, 2) 
    ws.set_column(2, 2, 14)    
    write_sheet_title(wb, ws, year_sort_list, month_sort_list, month_date_list)
    main_data_list = generate_excel_data(RawData_all, city, time_list)
    write_into_excel(wb, ws, main_data_list)

#反欺诈人员汇总
def select_dataframe(RawData_all, per_column, per_list):
    unit_dataframe = RawData_all[RawData_all[per_column] == per_list]
    return unit_dataframe
    
def per_get_count_type_list(RawData_all, per_column, per_list, column_name, time_list):
    unit_dataframe = select_dataframe(RawData_all, per_column, per_list)
    unit_dataframe = unit_dataframe[unit_dataframe[column_name].notnull()]
    cnt_list = []
    for t, tl in enumerate(time_list):
        for i, td in enumerate(tl):
            if t == 0:
                cnt_frame = unit_dataframe
            else:
                cnt_frame = unit_dataframe[unit_dataframe[column_name].str.contains(td)] 
            cnt_list.append(cnt_frame['ID'].nunique())
    return cnt_list
    
def generate_excel_data(unit_dataframe, per, time_list):
    #规则引擎报警量
    sys_riskdata = unit_dataframe[(unit_dataframe['score'] >= 130) & (unit_dataframe['userb'].notnull())]
    sysrisk_cnt_list = city_get_count_type_list(sys_riskdata, 'userb', per, 'usere_eo', time_list)
    #规则引擎处理量（确认欺诈、退回初审、提交终审）
    sys_fraud_cnt_list = city_get_count_type_list(sys_riskdata, 'userb', per, 'userb_tf', time_list)
    sys_back_cnt_list = city_get_count_type_list(sys_riskdata, 'userb', per, 'userb_ta', time_list)
    sys_fback_cnt_list = city_get_count_type_list(sys_riskdata, 'userb', per, 'userb_tc', time_list)
    #审批提报反欺诈量
    usera_tf_cnt_list = city_get_count_type_list(unit_dataframe, 'fraud', per, u'usera_tf', time_list)
    userc_tf_cnt_list = city_get_count_type_list(unit_dataframe, 'fraud', per, u'userc_tf', time_list)
    #审批提报处理量（确认欺诈、退回初审、退回终审）
    credit_data = unit_dataframe[(unit_dataframe['userb_tf'].isnull()) & ((unit_dataframe['fraud_result'] == u'初审黑名单确认') | (unit_dataframe['fraud_result'] == u'终审黑名单确认'))]
    credit_fraud_cnt_list = city_get_count_type_list(credit_data, 'fraud', per, u'decdate', time_list)
    
    usera_back_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['fraud_result'] == u'初审黑名单退回'], 'fraud', per, 'usera_back', time_list)
    userc_back_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['fraud_result'] == u'终审黑名单退回'], 'fraud', per, 'userc_back', time_list)

    #城市抽查
    sample_cnt_list = city_get_count_type_list(unit_dataframe, 'sa_staff', per, 'sa_date', time_list)
    sample_risk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['sa_risk'] == 'Y'], 'sa_staff', per, 'sa_date', time_list)
    sample_norisk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['sa_risk'] == 'N'], 'sa_staff', per, 'sa_date', time_list)
    #城市调查
    survey_cnt_list = city_get_count_type_list(unit_dataframe, 'su_staff', per, 'su_date', time_list)
    survey_risk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['su_risk'] == 'Y'], 'su_staff', per, 'su_date', time_list)
    survey_norisk_cnt_list = city_get_count_type_list(unit_dataframe[unit_dataframe['su_risk'] == 'N'], 'su_staff', per, 'su_date', time_list)
    
    #欺诈总量
    total_fraud_data = unit_dataframe[(unit_dataframe['status'] == u'反欺诈确认') & (unit_dataframe['des'].notnull())]
    total_fraud_cnt_list = city_get_count_type_list(total_fraud_data, 'fraud', per, 'decdate', time_list)
    #黑灰名单
    black_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（黑）')]
    black_cnt_list = city_get_count_type_list(black_data, 'fraud', per, u'decdate', time_list)
    
    gray_data = total_fraud_data[total_fraud_data['des'].str.contains(u'（灰）')]
    gray_cnt_list = city_get_count_type_list(gray_data, 'fraud', per, 'decdate', time_list)

    #规则引擎处理量
    sys_cnt_list = map(lambda(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list): sys_fraud_cnt_list + sys_back_cnt_list + sys_fback_cnt_list, zip(sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list))
    #审批提报处理量
    credit_cnt_list = map(lambda(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list): credit_fraud_cnt_list + usera_back_cnt_list + userc_back_cnt_list, zip(credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list))
    #总处理量
    fraud_dec_cnt_list = map(lambda(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list): sys_cnt_list + credit_cnt_list + sample_cnt_list + survey_cnt_list, zip(sys_cnt_list, credit_cnt_list, sample_cnt_list, survey_cnt_list))
    
    #总欺诈率=（规则引擎处理量确认欺诈+审批提报处理量确认欺诈+城市抽查确认欺诈风险+城市调查确认欺诈风险）/反欺诈总处理量
    fraud_cnt_list = map(lambda(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list): sys_fraud_cnt_list + credit_fraud_cnt_list + sample_risk_cnt_list + survey_risk_cnt_list, zip(sys_fraud_cnt_list, credit_fraud_cnt_list, sample_risk_cnt_list, survey_risk_cnt_list))
    fraud_per_list = division_calculator(fraud_cnt_list, fraud_dec_cnt_list)   
    #规则引擎欺诈率=规则引擎处理量确认欺诈/规则引擎处理量
    sys_fraud_per_list = division_calculator(sys_fraud_cnt_list, sys_cnt_list) 
    #审批提报欺诈率=审批提报处理量确认欺诈/审批提报处理量
    credit_fraud_per_list = division_calculator(credit_fraud_cnt_list, credit_cnt_list)
    #城市抽查欺诈率=城市抽查确认欺诈风险量/城市抽查量
    sa_fraud_per_list = division_calculator(sample_risk_cnt_list, sample_cnt_list)
    #城市调查欺诈率
    su_fraud_per_list = division_calculator(survey_risk_cnt_list, survey_cnt_list)
    #规则引擎回退率=规则引擎退回初审量/规则引擎处理量
    sys_back_per_list = division_calculator(sys_back_cnt_list, sys_cnt_list)
    #审批提报回退率=（审批提报处理量退回初审+审批提报处理量退回终审）/审批提报处理量
    credit_back_cnt_list = map(lambda(usera_back_cnt_list, userc_back_cnt_list): usera_back_cnt_list + userc_back_cnt_list, zip(usera_back_cnt_list, userc_back_cnt_list))
    credit_back_per_list = division_calculator(credit_back_cnt_list, credit_cnt_list)
    
    
    dbbz_data = total_fraud_data[total_fraud_data['des'] == u'代办包装（黑）']
    dbbz_cnt_list = city_get_count_type_list(dbbz_data, 'fraud', per, 'decdate', time_list)
    
    wmsq_data = total_fraud_data[total_fraud_data['des'] == u'伪冒申请（黑）']
    wmsq_cnt_list = city_get_count_type_list(wmsq_data, 'fraud', per, u'decdate', time_list)
    
    ztpd_data = total_fraud_data[total_fraud_data['des'] == u'组团骗贷（黑）']
    ztpd_cnt_list = city_get_count_type_list(ztpd_data, 'fraud', per, 'decdate', time_list)
    
    zlxj_data = total_fraud_data[total_fraud_data['des'] == u'资料虚假（黑）']
    zlxj_cnt_list = city_get_count_type_list(zlxj_data, 'fraud', per, 'decdate', time_list)
    
    zyxj_data = total_fraud_data[total_fraud_data['des'] == u'职业信息虚假（灰）']
    zyxj_cnt_list = city_get_count_type_list(zyxj_data, 'fraud', per, 'decdate', time_list)
    
    lzsq_data = total_fraud_data[total_fraud_data['des'] == u'离职后申请（灰）']
    lzsq_cnt_list = city_get_count_type_list(lzsq_data, 'fraud', per, 'decdate', time_list)
    
    dwxj_data = total_fraud_data[total_fraud_data['des'] == u'虚假单位（灰）']
    dwxj_cnt_list = city_get_count_type_list(dwxj_data, 'fraud', per, 'decdate', time_list)
    
    blsh_data = total_fraud_data[total_fraud_data['des'] == u'不良嗜好（灰）']
    blsh_cnt_list = city_get_count_type_list(blsh_data, 'fraud', per, 'decdate', time_list)
    
    whknl_data = total_fraud_data[total_fraud_data['des'] == u'无还款能力（灰）']
    whknl_cnt_list = city_get_count_type_list(whknl_data, 'fraud', per, 'decdate', time_list)
    
    cqtq_data = total_fraud_data[total_fraud_data['des'] == u'长期拖欠（灰）']
    cqtq_cnt_list = city_get_count_type_list(cqtq_data, 'fraud', per, 'decdate', time_list)
    
    ytfx_data = total_fraud_data[total_fraud_data['des'] == u'用途风险（灰）']
    ytfx_cnt_list = city_get_count_type_list(ytfx_data, 'fraud', per, 'decdate', time_list)
    
    qt_data = total_fraud_data[total_fraud_data['des'] == u'其他（灰）']
    qt_cnt_list = city_get_count_type_list(qt_data, 'fraud', per, 'decdate', time_list)
    
    main_data_list = [fraud_dec_cnt_list, sysrisk_cnt_list, sys_fraud_cnt_list, sys_back_cnt_list, sys_fback_cnt_list, usera_tf_cnt_list
                     , userc_tf_cnt_list, credit_fraud_cnt_list, usera_back_cnt_list, userc_back_cnt_list, sample_risk_cnt_list
                     , sample_norisk_cnt_list, survey_risk_cnt_list, survey_norisk_cnt_list, total_fraud_cnt_list, black_cnt_list
                     , gray_cnt_list, fraud_per_list, sys_fraud_per_list, credit_fraud_per_list, sa_fraud_per_list, su_fraud_per_list
                     , sys_back_per_list, credit_back_per_list, dbbz_cnt_list, wmsq_cnt_list, ztpd_cnt_list, zlxj_cnt_list, zyxj_cnt_list
                     , lzsq_cnt_list, dwxj_cnt_list, blsh_cnt_list, whknl_cnt_list, cqtq_cnt_list, ytfx_cnt_list, qt_cnt_list]
                                      
    return main_data_list

fraud_data = RawData_all[(RawData_all['status'] == u'反欺诈确认') & (RawData_all['des'].notnull())]    
userb_list = list(RawData_all[RawData_all['userb_tf'] >= '2017-02-01']['userb'].unique())
fraud_list = list(fraud_data[fraud_data['decdate'] >= '2017-02-01']['fraud'].unique())
sa_staff_list = list(RawData_all[RawData_all['sa_date'] >= '2017-01-01']['sa_staff'].unique())
su_staff_list = list(RawData_all[RawData_all['su_date'] >= '2017-01-01']['su_staff'].unique())
per_list = []
per_list.extend(userb_list)
per_list.extend(fraud_list)
per_list.extend(sa_staff_list)
per_list.extend(su_staff_list)
per_sort_list = list(set(per_list))
per_sort_list.sort()

#城市汇总-系统
for p, psl in enumerate(per_sort_list):
    ws = wb.add_worksheet(psl)
    ws.freeze_panes(2, 4)
    ws.set_column(0, 0, 2)
    ws.set_column(2, 2, 14)
         
    write_sheet_title(wb, ws, year_sort_list, month_sort_list, month_date_list)
    main_data_list = generate_excel_data(RawData_all, psl, time_list)
    write_into_excel(wb, ws, main_data_list)
    
    
wb.close()



