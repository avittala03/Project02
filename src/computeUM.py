import constants as const
import pandas as pd
import json
import copy
from scipy import sparse
from scipy.sparse import csr_matrix
import pickle
import numpy as np
import logging as logger
import sqlite3
import htmlmin
try:
    from app import app
except :
    pass
import csv
import time


def clean_raw_data(raw_data, string_cols):
    for col in string_cols:
        raw_data[col] = raw_data[col].apply(str)
    return raw_data


# function for exact match
# ignore '*'
def exact_match(col_list1, col_list2, compare_table):
    output_array = np.zeros(compare_table.shape[0])
    for col_name1 in col_list1:
        compare_table[col_name1] = compare_table[col_name1].str.replace(' ', '')
        for col_name2 in col_list2:
            compare_table[col_name2] = compare_table[col_name2].str.replace(' ', '')
            output_array = output_array + np.array((compare_table[col_name1] == compare_table[col_name2]) & (compare_table[col_name1] != '*'))
    return (output_array > 0) * 1


# function for date compare of overlap, partial overlap, no overlap
def date_match(dateA1, dateA2, dateB1, dateB2, compare_table):
    no_overlap = np.array((compare_table[dateA1] > compare_table[dateB2]) | (compare_table[dateB1] > compare_table[dateA2]))
    complete_overlap = np.array(((compare_table[dateA1] >= compare_table[dateB1]) & (compare_table[dateA2] <= compare_table[dateB2])) | ((compare_table[dateB1] >= compare_table[dateA1]) & (compare_table[dateB2] <= compare_table[dateA2])))
    partial_overlap = np.array((~no_overlap) & (~complete_overlap))
    return (no_overlap) * 1, (complete_overlap) * 1, (partial_overlap) * 1


def subset_header(x, claim_merged):
    claim_merged = claim_merged[(claim_merged.KEY_CHK_DCN_NBR == x.KEY_CHK_DCN_NBR) & (claim_merged.DTL_LINE_NBR == x.DTL_LINE_NBR)].reset_index(drop=True)
    claim_merged['SRC_PROV_NM'] = claim_merged['SRC_PROV_FRST_NM'] + ' ' + claim_merged['SRC_PROV_LAST_NM']
    claim_merged['SRC_PROV_NM_RP'] = claim_merged['SRC_PROV_FRST_NM_RP'] + ' ' + claim_merged['SRC_PROV_LAST_NM_RP']
    header_name = ['DCN/Case Number',  'Provider TaxID', 'Provider NPI', 'Provider Medicare#', 'Provider Name', 'Provider Second Name', 'Member Code', 'From Date', 'To Date', 'Claim Type/Case Code', 'Case Status']
    header_claim = ['KEY_CHK_DCN_NBR', 'PROV_TAX_ID', 'BILLG_NPI', '-', 'PROV_NM','PROV_SCNDRY_NM', 'PAT_MBR_CD', 'clm_from_dt', 'clm_to_dt', 'CLAIM_TYPE', '-']
    header_um = ['RFRNC_NBR', 'SRC_UM_PROV_ID', 'src_um_prov_id_rp', 'PROV_ID', 'SRC_PROV_NM', 'SRC_PROV_NM_RP', 'SRC_MBR_CD', 'AUTHRZD_SRVC_FROM_DT', 'AUTHRZD_SRVC_TO_DT', '-', 'SRC_UM_SRVC_STTS_CD']
    header_compare = {'Field': header_name,
                      'Claim': np.array(claim_merged[header_claim])[0],
                      'UM': np.array(claim_merged[header_um])[0]}
    header_compare = pd.DataFrame(header_compare)
    return htmlmin.minify(header_compare.to_html(index=False),remove_comments=True, remove_empty_space=True)


def auth_detail(x, claim_merged):
    detail_compare = claim_merged.loc[claim_merged.KEY_CHK_DCN_NBR == x.KEY_CHK_DCN_NBR,['RFRNC_NBR', 'RQSTD_PROC_SRVC_CD', 'AUTHRZD_PROC_SRVC_CD', 'AUTHRZD_SRVC_FROM_DT', 'AUTHRZD_SRVC_TO_DT', 'AUTHRZD_QTY']].rename(columns={'RFRNC_NBR': 'UM Case Number', 'RQSTD_PROC_SRVC_CD':'Procedure Code', 'AUTHRZD_PROC_SRVC_CD':'HCPCS Code', 'AUTHRZD_SRVC_FROM_DT':'From Date', 'AUTHRZD_SRVC_TO_DT':'To Date', 'AUTHRZD_QTY':'Units'})
    detail_compare = detail_compare.drop_duplicates()
    return htmlmin.minify(detail_compare.to_html(index=False),remove_comments=True, remove_empty_space=True)


def claim_detail(x, claim_merged):
    detail_compare = claim_merged.loc[claim_merged.KEY_CHK_DCN_NBR == x.KEY_CHK_DCN_NBR,['DTL_LINE_NBR','PROC_CD', 'HCPCS_CD', 'dtl_from_dt', 'dtl_to_dt', 'BILLD_SRVC_UNIT_QTY']].rename(columns={'DTL_LINE_NBR': 'Line #', 'PROC_CD':'Procedure Code', 'HCPCS_CD':'HCPCS Code', 'dtl_from_dt':'From Date', 'dtl_to_dt':'To Date', 'BILLD_SRVC_UNIT_QTY':'Units'})
    detail_compare = detail_compare.drop_duplicates()
    detail_compare = pd.DataFrame(detail_compare)
    detail_compare = pd.DataFrame(detail_compare)
    return htmlmin.minify(detail_compare.to_html(index=False),remove_comments=True, remove_empty_space=True)


# function for column contain column
# ignore '*'
def contain_match(col_list1, col_list2, compare_table):
    output_array = np.zeros(compare_table.shape[0])
    for col_name1 in col_list1:
        if pd.isna(compare_table[col_name1].str.len().unique())[0]:
            continue
        for col_name2 in col_list2: #np.array(col_locator['VAR'].str.startswith("PROV_TAX_ID"))
            if pd.isna(compare_table[col_name2].str.len().unique()[0]):
                continue
            output_array = output_array + np.array((compare_table.apply(lambda x: x[col_name1] in x[col_name2], axis=1)) & (compare_table[col_name1] != '*') & (compare_table[col_name2] != '*'))
    return (output_array > 0) * 1


def UM_deploy(um_auth_data, claim_line_data, model_name):
    start_time = time.time()
    um_auth_data = um_auth_data.loc[:, ['SRC_SBSCRBR_ID', 'RFRNC_NBR', 'SRVC_LINE_NBR', 'PRMRY_DIAG_CD', 'SRC_MBR_CD', 'RQSTD_PLACE_OF_SRVC_CD', 'SRC_RQSTD_PLACE_OF_SRVC_CD', 'AUTHRZD_PLACE_OF_SRVC_CD', 'SRC_AUTHRZD_PLACE_OF_SRVC_CD', 'RQSTD_PROC_SRVC_CD', 'AUTHRZD_PROC_SRVC_CD', 'RQSTD_SRVC_FROM_DT', 'RQSTD_SRVC_TO_DT', 'AUTHRZD_SRVC_FROM_DT', 'AUTHRZD_SRVC_TO_DT', 'SRC_UM_PROV_ID', 'src_um_prov_id_rp', 'PROV_ID', 'prov_id_rp', 'RQSTD_QTY', 'AUTHRZD_QTY', 'SRC_PROV_FRST_NM_RP','SRC_PROV_LAST_NM_RP', 'SRC_PROV_FRST_NM', 'SRC_PROV_LAST_NM', 'SRC_UM_SRVC_STTS_CD']]
    um_auth_data = um_auth_data.drop_duplicates().reset_index(drop=True)

    claim_line_data = claim_line_data.loc[:, ['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'action_code', 'DTL_LINE_NBR', 'UM_CASE_NBR', 'memID', 'PROV_NM', 'PROV_SCNDRY_NM', 'ICD_A_CD', 'ICD_B_CD', 'ICD_C_CD', 'ICD_D_CD', 'ICD_E_CD', 'ICD_OTHR_6_CD', 'ICD_OTHR_7_CD', 'ICD_OTHR_8_CD', 'ICD_OTHR_9_CD', 'ICD_OTHR_10_CD', 'ICD_OTHR_11_CD', 'ICD_OTHR_12_CD', 'ICD_OTHR_13_CD', 'ICD_OTHR_14_CD', 'ICD_OTHR_15_CD', 'ICD_OTHR_16_CD', 'ICD_OTHR_17_CD', 'ICD_OTHR_18_CD', 'ICD_OTHR_19_CD', 'ICD_OTHR_20_CD', 'PAT_MBR_CD', 'HCFA_PT_CD', 'POT_CD', 'PROC_CD', 'HCPCS_CD', 'clm_from_dt', 'clm_to_dt', 'dtl_from_dt', 'dtl_to_dt', 'PROV_TAX_ID', 'BILLD_SRVC_UNIT_QTY', 'CLAIM_TYPE', 'BILLG_NPI', 'TOS_TYPE_CD']]
    claim_line_data = claim_line_data.drop_duplicates().reset_index(drop=True)

    um_auth_data = um_auth_data.fillna('*')
    claim_line_data = claim_line_data.fillna('*')

    um_auth_data = clean_raw_data(um_auth_data, ['SRC_SBSCRBR_ID', 'RFRNC_NBR', 'SRVC_LINE_NBR', 'PRMRY_DIAG_CD', 'SRC_MBR_CD', 'RQSTD_PLACE_OF_SRVC_CD', 'SRC_RQSTD_PLACE_OF_SRVC_CD', 'AUTHRZD_PLACE_OF_SRVC_CD', 'SRC_AUTHRZD_PLACE_OF_SRVC_CD', 'RQSTD_PROC_SRVC_CD', 'AUTHRZD_PROC_SRVC_CD', 'RQSTD_SRVC_FROM_DT', 'RQSTD_SRVC_TO_DT', 'AUTHRZD_SRVC_FROM_DT', 'AUTHRZD_SRVC_TO_DT', 'SRC_UM_PROV_ID', 'src_um_prov_id_rp', 'PROV_ID', 'prov_id_rp', 'RQSTD_QTY', 'AUTHRZD_QTY', 'SRC_PROV_FRST_NM_RP','SRC_PROV_LAST_NM_RP', 'SRC_PROV_FRST_NM', 'SRC_PROV_LAST_NM', 'SRC_UM_SRVC_STTS_CD'])
    claim_line_data = clean_raw_data(claim_line_data, ['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'action_code', 'DTL_LINE_NBR', 'UM_CASE_NBR', 'memID', 'PROV_NM', 'PROV_SCNDRY_NM', 'ICD_A_CD', 'ICD_B_CD', 'ICD_C_CD', 'ICD_D_CD', 'ICD_E_CD', 'ICD_OTHR_6_CD', 'ICD_OTHR_7_CD', 'ICD_OTHR_8_CD', 'ICD_OTHR_9_CD', 'ICD_OTHR_10_CD', 'ICD_OTHR_11_CD', 'ICD_OTHR_12_CD', 'ICD_OTHR_13_CD', 'ICD_OTHR_14_CD', 'ICD_OTHR_15_CD', 'ICD_OTHR_16_CD', 'ICD_OTHR_17_CD', 'ICD_OTHR_18_CD', 'ICD_OTHR_19_CD', 'ICD_OTHR_20_CD', 'PAT_MBR_CD', 'HCFA_PT_CD', 'POT_CD', 'PROC_CD', 'HCPCS_CD', 'clm_from_dt', 'clm_to_dt', 'dtl_from_dt', 'dtl_to_dt', 'PROV_TAX_ID', 'BILLD_SRVC_UNIT_QTY', 'CLAIM_TYPE', 'BILLG_NPI', 'TOS_TYPE_CD'])
    
    # merge claim line data and csv
    claim_merged = claim_line_data.merge(um_auth_data, how='inner', left_on='memID', right_on='SRC_SBSCRBR_ID')

    # print(claim_merged.columns)

    print('no of claim_merged rows %d' % len(claim_merged))

    # prepare output table
    matching_result = claim_merged.loc[:, ['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'DTL_LINE_NBR', 'UM_CASE_NBR', 'RFRNC_NBR']]

    # diag code exact match
    col_list1 = ['ICD_A_CD', 'ICD_B_CD', 'ICD_C_CD', 'ICD_D_CD', 'ICD_E_CD', 'ICD_OTHR_6_CD', 'ICD_OTHR_7_CD', 'ICD_OTHR_8_CD', 'ICD_OTHR_9_CD', 'ICD_OTHR_10_CD', 'ICD_OTHR_11_CD', 'ICD_OTHR_12_CD', 'ICD_OTHR_13_CD', 'ICD_OTHR_14_CD', 'ICD_OTHR_15_CD', 'ICD_OTHR_16_CD', 'ICD_OTHR_17_CD', 'ICD_OTHR_18_CD', 'ICD_OTHR_19_CD', 'ICD_OTHR_20_CD']
    col_list2 = ['PRMRY_DIAG_CD']
    matching_result.loc[:, 'Diag_code_exact'] = exact_match(col_list1, col_list2, claim_merged)
    
    # member code exact match
    col_list1 = ['PAT_MBR_CD']
    col_list2 = ['SRC_MBR_CD']
    matching_result.loc[:, 'Member_code_exact'] = exact_match(col_list1, col_list2, claim_merged)

    # place of service exact match
    col_list1 = ['HCFA_PT_CD', 'POT_CD']
    # print(claim_merged.loc[:, col_list1])
    col_list2 = ['RQSTD_PLACE_OF_SRVC_CD', 'SRC_RQSTD_PLACE_OF_SRVC_CD', 'AUTHRZD_PLACE_OF_SRVC_CD', 'SRC_AUTHRZD_PLACE_OF_SRVC_CD']
    matching_result.loc[:, 'Place_of_service_exact'] = exact_match(col_list1, col_list2, claim_merged)

    # proc code exact match
    col_list1 = ['PROC_CD', 'HCPCS_CD']
    col_list2 = ['RQSTD_PROC_SRVC_CD', 'AUTHRZD_PROC_SRVC_CD']
    matching_result.loc[:, 'Proc_code_exact'] = exact_match(col_list1, col_list2, claim_merged)    

    # date format change
    claim_merged['clm_from_dt2'] = pd.to_datetime(claim_merged['clm_from_dt'], format='%m/%d/%Y', errors='coerce')
    claim_merged['clm_to_dt2'] = pd.to_datetime(claim_merged['clm_to_dt'], format='%m/%d/%Y', errors='coerce')
    claim_merged['dtl_from_dt2'] = pd.to_datetime(claim_merged['dtl_from_dt'], format='%m/%d/%Y', errors='coerce')
    claim_merged['dtl_to_dt2'] = pd.to_datetime(claim_merged['dtl_to_dt'], format='%m/%d/%Y', errors='coerce')

    if '/' in claim_merged['RQSTD_SRVC_FROM_DT'][0]:
        claim_merged['RQSTD_SRVC_FROM_DT2'] = pd.to_datetime(claim_merged['RQSTD_SRVC_FROM_DT'], format='%m/%d/%Y', errors='coerce')
        claim_merged['RQSTD_SRVC_TO_DT2'] = pd.to_datetime(claim_merged['RQSTD_SRVC_TO_DT'], format='%m/%d/%Y', errors='coerce')
        claim_merged['AUTHRZD_SRVC_FROM_DT2'] = pd.to_datetime(claim_merged['AUTHRZD_SRVC_FROM_DT'], format='%m/%d/%Y', errors='coerce')
        claim_merged['AUTHRZD_SRVC_TO_DT2'] = pd.to_datetime(claim_merged['AUTHRZD_SRVC_TO_DT'], format='%m/%d/%Y', errors='coerce')
    elif '-' in claim_merged['RQSTD_SRVC_FROM_DT'][0]:
        claim_merged['RQSTD_SRVC_FROM_DT2'] = pd.to_datetime(claim_merged['RQSTD_SRVC_FROM_DT'], format='%Y-%m-%d', errors='coerce')
        claim_merged['RQSTD_SRVC_TO_DT2'] = pd.to_datetime(claim_merged['RQSTD_SRVC_TO_DT'], format='%Y-%m-%d', errors='coerce')
        claim_merged['AUTHRZD_SRVC_FROM_DT2'] = pd.to_datetime(claim_merged['AUTHRZD_SRVC_FROM_DT'], format='%Y-%m-%d', errors='coerce')
        claim_merged['AUTHRZD_SRVC_TO_DT2'] = pd.to_datetime(claim_merged['AUTHRZD_SRVC_TO_DT'], format='%Y-%m-%d', errors='coerce')
    claim_merged.loc[pd.isnull(claim_merged.loc[:, 'AUTHRZD_SRVC_FROM_DT2']), 'AUTHRZD_SRVC_FROM_DT2'] = claim_merged.loc[pd.isnull(claim_merged.loc[:, 'AUTHRZD_SRVC_FROM_DT2']), 'RQSTD_SRVC_FROM_DT2']
    claim_merged.loc[pd.isnull(claim_merged.loc[:, 'AUTHRZD_SRVC_TO_DT2']), 'AUTHRZD_SRVC_TO_DT2'] = claim_merged.loc[pd.isnull(claim_merged.loc[:, 'AUTHRZD_SRVC_TO_DT2']), 'RQSTD_SRVC_TO_DT2']

    # date overlap
    matching_result.loc[:, 'clm_RQSTD_no_overlap'], matching_result.loc[:, 'clm_RQSTD_complete_overlap'], matching_result.loc[:, 'clm_RQSTD_partial_overlap'] = date_match('clm_from_dt2', 'clm_to_dt2', 'RQSTD_SRVC_FROM_DT2', 'RQSTD_SRVC_TO_DT2', claim_merged)
    matching_result.loc[:, 'clm_AUTHRZD_no_overlap'], matching_result.loc[:, 'clm_AUTHRZD_complete_overlap'], matching_result.loc[:, 'clm_AUTHRZD_partial_overlap'] = date_match('clm_from_dt2', 'clm_to_dt2', 'AUTHRZD_SRVC_FROM_DT2', 'AUTHRZD_SRVC_TO_DT2', claim_merged)
    matching_result.loc[:, 'dtl_RQSTD_no_overlap'], matching_result.loc[:, 'dtl_RQSTD_complete_overlap'], matching_result.loc[:, 'dtl_RQSTD_partial_overlap'] = date_match('dtl_from_dt2', 'dtl_to_dt2', 'RQSTD_SRVC_FROM_DT2', 'RQSTD_SRVC_TO_DT2', claim_merged)
    matching_result.loc[:, 'dtl_AUTHRZD_no_overlap'], matching_result.loc[:, 'dtl_AUTHRZD_complete_overlap'], matching_result.loc[:, 'dtl_AUTHRZD_partial_overlap'] = date_match('dtl_from_dt2', 'dtl_to_dt2', 'AUTHRZD_SRVC_FROM_DT2', 'AUTHRZD_SRVC_TO_DT2', claim_merged)
    # dtl_from_dt - AUTHRZD_SRVC_FROM_DT difference
    matching_result.loc[:, 'dtl_AUTHRZD_from_dt'] = (claim_merged['dtl_from_dt2'] - claim_merged['AUTHRZD_SRVC_FROM_DT2']).dt.days
    matching_result.loc[pd.isnull(matching_result.loc[:, 'dtl_AUTHRZD_from_dt']), 'dtl_AUTHRZD_from_dt'] = -100
    # AUTHRZD_SRVC_TO_DT - dtl_to_dt difference
    matching_result.loc[:, 'AUTHRZD_dtl_to_dt'] = (claim_merged['AUTHRZD_SRVC_TO_DT2'] - claim_merged['dtl_to_dt2']).dt.days
    matching_result.loc[pd.isnull(matching_result.loc[:, 'AUTHRZD_dtl_to_dt']), 'AUTHRZD_dtl_to_dt'] = -100

    # tax id contain match
    col_list1 = ['PROV_TAX_ID', 'BILLG_NPI']
    col_list2 = ['SRC_UM_PROV_ID', 'src_um_prov_id_rp', 'PROV_ID', 'prov_id_rp']
    matching_result.loc[:, 'Tax_id_contain'] = contain_match(col_list1, col_list2, claim_merged)

    # PROV_NM contain match
    claim_merged['SRC_PROV_NM'] = claim_merged['SRC_PROV_FRST_NM'] + ' ' + claim_merged['SRC_PROV_LAST_NM']
    claim_merged['SRC_PROV_NM_RP'] = claim_merged['SRC_PROV_FRST_NM_RP'] + ' ' + claim_merged['SRC_PROV_LAST_NM_RP']
    claim_merged['SRC_PROV_NM'] = claim_merged['SRC_PROV_NM'].str.lower()
    claim_merged['SRC_PROV_NM_RP'] = claim_merged['SRC_PROV_NM_RP'].str.lower()
    claim_merged['PROV_NM2'] = claim_merged['PROV_NM'].str.lower()
    claim_merged['PROV_SCNDRY_NM2'] = claim_merged['PROV_SCNDRY_NM'].str.lower()
    col_list1 = ['PROV_NM2', 'PROV_SCNDRY_NM2']
    col_list2 = ['SRC_PROV_NM', 'SRC_PROV_NM_RP']
    matching_result.loc[:, 'PROV_NM_contain'] = contain_match(col_list1, col_list2, claim_merged)

    # status == APPRVC
    matching_result.loc[:, 'SRC_UM_SRVC_STTS_CD_APPRVC'] = np.array(claim_merged['SRC_UM_SRVC_STTS_CD'].str.startswith('APPRVD')) * 1

    # tax id is NA
    tax_id_NA_list = ['*','UNK','999999999999','UMSHELLPRAC','UMSHELLFAC','Unknown','############','999999999','UUUUU1']
    col_list2 = ['SRC_UM_PROV_ID', 'src_um_prov_id_rp', 'PROV_ID', 'prov_id_rp']
    for col_name2 in col_list2:
        for tax_id_NA_item in tax_id_NA_list:
            claim_merged[col_name2] = claim_merged[col_name2].str.replace(tax_id_NA_item, 'UNK')
    matching_result.loc[:, 'Tax_id_NA'] = np.array(claim_merged.SRC_UM_PROV_ID.str.startswith('UNK') & claim_merged.src_um_prov_id_rp.str.startswith('UNK') & claim_merged.PROV_ID.str.startswith('UNK') & claim_merged.prov_id_rp.str.startswith('UNK')) * 1

    # TOS_TYPE_CD
    TOS_TYPE_CD_list = ['OPH','PHT','DXL','PSY','SUP','HHC','RAB','SRG','PHO','DME','SPC','RAD','MWH','PRV','HSP','CHE','PAN','AMB','SPT','PAS','SNF','AIR','ERP','CON','DEN']
    for TOS_TYPE_CD_item in TOS_TYPE_CD_list:
        matching_result.loc[:, 'TOS_TYPE_CD_'+TOS_TYPE_CD_item] = np.array(claim_merged['TOS_TYPE_CD']==TOS_TYPE_CD_item) * 1

    # qty format change
    claim_merged['BILLD_SRVC_UNIT_QTY'] = pd.to_numeric(claim_merged['BILLD_SRVC_UNIT_QTY'], downcast='float', errors='coerce')
    claim_merged['RQSTD_QTY'] = pd.to_numeric(claim_merged['RQSTD_QTY'], downcast='float', errors='coerce')
    claim_merged['AUTHRZD_QTY'] = pd.to_numeric(claim_merged['AUTHRZD_QTY'], downcast='float', errors='coerce')
    # qty difference
    matching_result.loc[:, 'Qty_diff_RQSTD'] = claim_merged['RQSTD_QTY'] - claim_merged['BILLD_SRVC_UNIT_QTY']
    matching_result.loc[:, 'Qty_diff_AUTHRZD'] = claim_merged['AUTHRZD_QTY'] - claim_merged['BILLD_SRVC_UNIT_QTY']

    # claim_type variable
    matching_result.loc[:, 'CLAIM_TYPE_INPT'] = (claim_merged['CLAIM_TYPE'] == 'INPT') * 1
    matching_result.loc[:, 'CLAIM_TYPE_PROF'] = (claim_merged['CLAIM_TYPE'] == 'PROF') * 1
    matching_result.loc[:, 'CLAIM_TYPE_SN'] = (claim_merged['CLAIM_TYPE'] == 'SN') * 1
    matching_result.loc[:, 'CLAIM_TYPE_OUTPT'] = (claim_merged['CLAIM_TYPE'] == 'OUTPT') * 1

    # clm_from_dt - AUTHRZD_SRVC_FROM_DT difference
    matching_result.loc[:, 'clm_AUTHRZD_from_dt'] = (claim_merged['clm_from_dt2'] - claim_merged['AUTHRZD_SRVC_FROM_DT2']).dt.days
    # AUTHRZD_SRVC_TO_DT - clm_to_dt difference
    matching_result.loc[:, 'AUTHRZD_clm_to_dt'] = (claim_merged['AUTHRZD_SRVC_TO_DT2'] - claim_merged['clm_to_dt2']).dt.days
    # dtl_from_dt - RQSTD_SRVC_FROM_DT difference
    matching_result.loc[:, 'dtl_RQSTD_from_dt'] = (claim_merged['dtl_from_dt2'] - claim_merged['RQSTD_SRVC_FROM_DT2']).dt.days
    # RQSTD_SRVC_TO_DT - dtl_to_dt difference
    matching_result.loc[:, 'RQSTD_dtl_to_dt'] = (claim_merged['RQSTD_SRVC_TO_DT2'] - claim_merged['dtl_to_dt2']).dt.days
    # clm_from_dt - RQSTD_SRVC_FROM_DT difference
    matching_result.loc[:, 'clm_RQSTD_from_dt'] = (claim_merged['clm_from_dt2'] - claim_merged['RQSTD_SRVC_FROM_DT2']).dt.days
    # RQSTD_SRVC_TO_DT - clm_to_dt difference
    matching_result.loc[:, 'RQSTD_clm_to_dt'] = (claim_merged['RQSTD_SRVC_TO_DT2'] - claim_merged['clm_to_dt2']).dt.days
    matching_result.loc[:,['clm_AUTHRZD_from_dt','AUTHRZD_clm_to_dt','dtl_RQSTD_from_dt','RQSTD_dtl_to_dt','clm_RQSTD_from_dt','RQSTD_clm_to_dt']] = matching_result.loc[:,['clm_AUTHRZD_from_dt','AUTHRZD_clm_to_dt','dtl_RQSTD_from_dt','RQSTD_dtl_to_dt','clm_RQSTD_from_dt','RQSTD_clm_to_dt']].fillna(-100)

    # output
    Y_col_list = ['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'DTL_LINE_NBR', 'UM_CASE_NBR','RFRNC_NBR']
    Y_data = matching_result.loc[:, Y_col_list]
    X_data = matching_result.drop(columns=Y_col_list)
    col_locator = pd.DataFrame({'VAR':pd.Series(X_data.columns.values)})
    col_locator['col'] = range(X_data.shape[1])
    X_data = sparse.csr_matrix(X_data.values)
    print('Create model data took', (time.time() - start_time) / 60.0, 'minutes')    
    
    start_time = time.time()
    if (model_name == None):
        rf = copy.deepcopy(app.config['models'][const.UM_MODEL_KEY])
    else:
        rf = pickle.load(open(model_name,'rb'))

    X_data = X_data[:, np.array((col_locator['VAR'] != 'Diag_code_exact') & (col_locator['VAR'] != 'Place_of_service_exact') & (~col_locator['VAR'].str.endswith('overlap')))]
    col_locator = col_locator.loc[np.array((col_locator['VAR'] != 'Diag_code_exact') & (col_locator['VAR'] != 'Place_of_service_exact') & (~col_locator['VAR'].str.endswith('overlap'))),:].reset_index(drop=True)
    
    pred_test = rf.predict_proba(X_data)[:, -1]
    Y_data['score'] = pred_test
    print('Prediction took', (time.time() - start_time) , 'seconds')

    # UM output
    start_time = time.time()
    Y_data_135 = Y_data.groupby(['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'DTL_LINE_NBR', 'UM_CASE_NBR', 'RFRNC_NBR'], as_index=False)['score'].max()
    Y_data_135 = Y_data_135.sort_values(['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR', 'score'], ascending=[True, True, True, False])
    Y_data_135['idx'] = Y_data_135.groupby(['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR']).cumcount()
    final_out = Y_data_135.groupby(['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR','UM_CASE_NBR'], as_index=False).agg({'idx': np.max}).rename(
        columns={'idx': 'num_potential_ums'})
    final_out['num_potential_ums'] = final_out['num_potential_ums'] + 1
    for i in range(3):
        idx = i + 1
        tmp = Y_data_135[Y_data_135['idx'] == i][['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR','RFRNC_NBR', 'score']].rename(
            columns={'RFRNC_NBR': 'Top_' + str(idx) + '_UM', 'score': 'Top_' + str(idx) + '_UM_Score'})
        final_out = pd.merge(final_out, tmp, how='left', on=['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR'])
    feature_data = pd.DataFrame(X_data.toarray(),columns=col_locator.VAR)
    feature_data = feature_data.reset_index(drop=True)
    score_feature_data = pd.concat([Y_data,feature_data],axis=1,join='inner')
    claim_merged['score'] = pred_test
    claim_merged['Tax_id_NA'] = matching_result['Tax_id_NA']
    print('Output result took', (time.time() - start_time) , 'seconds')

    # print(matching_result)
    return final_out, score_feature_data, claim_merged


def process_request(payload):
    start_time = time.time()
    claim_line_data = pd.DataFrame.from_dict([payload], orient='columns')
    conn = sqlite3.connect(const.DB_PATH)
    memID = claim_line_data.loc[:, ['memID'] ].iloc[0][0]
    um_auth_data = pd.read_sql_query('SELECT * FROM UM WHERE SRC_SBSCRBR_ID = ?', conn, params=(memID, ))
    # print(um_auth_data)
    print('Number of UM rows found %d' % len(um_auth_data))
    final_out, score_feature_data, claim_merged = UM_deploy(um_auth_data, claim_line_data, None)

    result_row = {}
    result_row['KEY_CHK_DCN_NBR'] = payload['KEY_CHK_DCN_NBR']
    result_row['CURNT_QUE_UNIT_NBR'] = payload['CURNT_QUE_UNIT_NBR']
    recommendations = []
    recommendation = {}
    recommendation['modelName'] = 'um'
    recommendation['actionCode'] = final_out['Top_1_UM'].iloc[0]
    print(type(final_out['Top_1_UM']))
    recommendation['actionValue'] = final_out['Top_1_UM_Score'].iloc[0]
    recommendations.append(recommendation)
    result_row['recommendations'] = recommendations
    
    return result_row


if __name__ == '__main__':
    claim_lines = []
    with open('/Users/Af81392/PycharmProjects/cognitive-claims/input/um/2019-02-26/test.csv', encoding='ISO-8859-1') as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames
        for row in reader:
            claim_lines.append({title[i]:row[title[i]] for i in range(len(title))})

    print ('Number of claims lines being processed %d' % len(claim_lines[0].keys()))
    # converts json list to dataframe
    start_time = time.time()
    claim_line_data = pd.DataFrame.from_dict(claim_lines, orient='columns')
    # print(claim_line_data)
    conn = sqlite3.connect('/Users/Af81392/PycharmProjects/cognitive-claims/data/cogx.db')
    model_name = '/Users/Af81392/PycharmProjects/cognitive-claims/models/um/201806_201810_date_RP_noDiagPlSRVCoverlap_gradient_boost_model.sav'
    output_file = '/Users/Af81392/PycharmProjects/cognitive-claims/output/um/UM_output_top3.csv'
    output_file_filtered = '/Users/Af81392/PycharmProjects/cognitive-claims/output/um/UM_output_filtered.csv'
    output_feature_file = '/Users/Af81392/PycharmProjects/cognitive-claims/output/um/UM_output_features.csv'
    output_diff_file = '/Users/Af81392/PycharmProjects/cognitive-claims/output/um/AdditionalDataButton.csv'
    for claim_line in claim_lines:
        memID = claim_line['memID']
        # print(memID)
        um_auth_data = pd.read_sql_query('SELECT * FROM UM WHERE SRC_SBSCRBR_ID = ?', conn, params=(memID, ))
        # print(um_auth_data)
        print('Number of UM rows found %d' % len(um_auth_data))
        final_out, score_feature_data, claim_merged = UM_deploy(um_auth_data, claim_line_data, model_name)

        # print(final_out)
        # print(score_feature_data)
        threshold = 0.5
        final_out.to_csv(output_file, index=False)
        final_out_filtered = final_out[final_out.Top_1_UM_Score >= threshold]
        final_out_filtered = final_out_filtered.sort_values(['KEY_CHK_DCN_NBR','Top_1_UM_Score'], ascending=[True, False])
        final_out_filtered = final_out_filtered[['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','Top_1_UM']].rename(columns={'Top_1_UM':'Result'}).drop_duplicates(subset=['KEY_CHK_DCN_NBR'])
        final_out_filtered.to_csv(output_file_filtered, index=False)
        score_feature_data.to_csv(output_feature_file, index=False)

        claim_merged = claim_merged.sort_values(['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR', 'score', 'Tax_id_NA'], ascending=[True, True, True, False, True])
        claim_merged = claim_merged.drop_duplicates(subset=['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR', 'DTL_LINE_NBR']).reset_index(drop=True)
        claim_merged['-'] = '-'
        claim_merged = claim_merged.loc[claim_merged.score >= threshold,:].reset_index(drop=True)
        if len(claim_merged) > 0 :
            claim_merged_output = claim_merged.loc[:,['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','DTL_LINE_NBR','UM_CASE_NBR', 'RFRNC_NBR','score']]
            claim_merged_output = claim_merged_output.sort_values(['KEY_CHK_DCN_NBR','score'], ascending=[True, False])
            claim_merged_output = claim_merged_output.drop_duplicates(subset=['KEY_CHK_DCN_NBR']).reset_index(drop=True)
            claim_merged_output['Header_compare'] = claim_merged_output.apply(subset_header, args=(claim_merged,), axis=1)
            claim_merged_output['auth_detail'] = claim_merged_output.apply(auth_detail, args=(claim_merged,), axis=1)
            claim_merged_output['claim_detail'] = claim_merged_output.apply(claim_detail, args=(claim_merged,), axis=1)
            claim_merged_output['Result_HTML'] = '<h4>Claim/Case Header</h4>' + claim_merged_output['Header_compare'] + '<br>' + '<h4>Auth data</h4>' + claim_merged_output['auth_detail'] + '<br>' + '<h4>Claim Data</h4>' + claim_merged_output['claim_detail']
            claim_merged_output = claim_merged_output.loc[:,['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR','Result_HTML']]
            claim_merged_output.to_csv(output_diff_file, index=False, line_terminator="\r\n")

    print('Create model data and executing model took', (time.time() - start_time), 'seconds') 
    print(claim_lines)
    '''
    for claim_line in claim_lines:
        memID = (claim_line['memID'],)
        c = conn.cursor()
        rows = c.execute("SELECT * FROM UM WHERE SRC_SBSCRBR_ID = ?", memID)
        for row in rows:
            print(row)
    '''