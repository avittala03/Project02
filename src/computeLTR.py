import constants as const
import pandas as pd
import json
import copy
import scipy.sparse as sp
import pickle
import numpy as np
import logging as logger
from app import app


error_codes = const.EDIT_DICT['ERR_CD'] 
icd_codes = const.HEADER_DICT['ICD']
mod_codes = const.DETAIL_DICT['MODIFIER_CD']


def claim_edit_processing(field_mapping):
    for err_cd in error_codes:
        if field_mapping[err_cd] != '*':
            field_mapping['ERR_CD_' + field_mapping[err_cd]] = 1.0
    return field_mapping


def claim_detail_processing(field_mapping):
    if (float(field_mapping.get('MEDCRB_APRVD_AMT', 0)) > 0 and 
        (float(field_mapping.get('MEDCRB_APRVD_AMT',  0)) == float(field_mapping.get('MEDCRB_PAID_AMT',  0))) ):        
        field_mapping['MEDCRB_APPV_EQU_PAID'] = 1.0
    if (float(field_mapping.get('MEDCRB_COINSRN_AMT', 0)) + float(field_mapping.get('MEDCRB_DDCTBL_AMT', 0))) == 0 :
        field_mapping['MEDCRB_TWO_ZERO'] = 1.0

    for mod_cd in mod_codes:
        if field_mapping[mod_cd] != '*':
            field_mapping['MODIFIER_CD_' + field_mapping[mod_cd]] = 1.0

        
    for col_name in const.DETAIL_DATE_COLS:
        if field_mapping[col_name] == '12/31/9999':
            field_mapping[col_name] = '12/31/2049' 
        elif field_mapping[col_name] == '9999-12-31':
            field_mapping[col_name] == '2049-12-31'
        if '/' in field_mapping[col_name]:
            field_mapping[col_name] = pd.to_datetime(field_mapping[col_name],format= '%m/%d/%Y')
        elif '-' in field_mapping[col_name]:
            field_mapping[col_name] = pd.to_datetime(field_mapping[col_name],format= '%Y-%m-%d')
    field_mapping['SRVC_OUT_BNFT'] = (field_mapping['SRVC_FROM_DT'] >= field_mapping['BNFT_YEAR_CNTRCT_REV_DT']) or (field_mapping['SRVC_TO_DT'] <= field_mapping['BNFT_YEAR_CNTRCT_EFCTV_DT'])
    field_mapping['SRVC_OVERLAP_BNFT'] = (field_mapping['SRVC_FROM_DT'] <= field_mapping['BNFT_YEAR_CNTRCT_REV_DT']) and (field_mapping['SRVC_TO_DT'] >= field_mapping['BNFT_YEAR_CNTRCT_EFCTV_DT'])
    field_mapping['SRVC_OUT_MBR'] = (field_mapping['SRVC_FROM_DT'] >= field_mapping['MBR_CNTRCT_END_DT']) or (field_mapping['SRVC_TO_DT'] <= field_mapping['MBR_CNTRCT_EFCTV_DT'])
    field_mapping['SRVC_OVERLAP_MBR'] = (field_mapping['SRVC_FROM_DT'] <= field_mapping['MBR_CNTRCT_END_DT']) and (field_mapping['SRVC_TO_DT'] <= field_mapping['MBR_CNTRCT_EFCTV_DT'])
    return field_mapping


def claim_header_processing(field_mapping):
    for icd_cd in icd_codes:
        if field_mapping[icd_cd] != '*':
            field_mapping['ICD_' + field_mapping[icd_cd]] = 1.0
    return field_mapping


def process_request(payload):
    field_mapping = {}
    test_row = {}  # return value
    field_mapping['MEDCRB_APPV_EQU_PAID'] = 0.0
    field_mapping['MEDCRB_TWO_ZERO'] = 0.0
    
    field_mapping['SRVC_OUT_BNFT'] = 0.0
    field_mapping['SRVC_OVERLAP_BNFT'] = 0.0
    field_mapping['SRVC_OUT_MBR'] = 0.0
    field_mapping['SRVC_OVERLAP_MBR'] = 0.0
    
    logger.debug(payload)
    for key in payload.keys():
        content = payload[key]
        if isinstance(content, dict):
            for k, v in content.items():
                field_mapping[k.strip()] = v.strip()
                logger.debug(k.strip())
                field_mapping[k.strip() + '_' + v.strip()] = 1.0
                logger.debug(k.strip() + '_' + v.strip())
            if key == 'header':
                field_mapping = claim_header_processing(field_mapping)
                test_row['KEY_CHK_DCN_NBR'] = field_mapping['KEY_CHK_DCN_NBR']
                test_row['CURNT_QUE_UNIT_NBR'] = field_mapping['CURNT_QUE_UNIT_NBR']
        elif isinstance(content, list):
            for elem in content:
                if isinstance(elem, dict):
                    for k, v in elem.items():
                        field_mapping[k.strip()] = v.strip()
                        # logger.debug(k.strip())
                        field_mapping[k.strip() + '_' + v.strip()] = 1.0
                        # logger.debug(k.strip() + '_' + v.strip())
                    if key == 'edits':
                        field_mapping = claim_edit_processing(field_mapping)
                    elif key == 'details':
                        field_mapping = claim_detail_processing(field_mapping)

    logger.debug(field_mapping)

    values = []
    cols = []
    reject_list = app.config['model_encodings']['reject_list']
    for idx, value in enumerate(reject_list):
        if field_mapping.get(value) is not None:
            # logger.debug(value)
            # print(value + ' -'  + str(idx-1) + ' - ' + str(float(field_mapping.get(value))))
            values.append(float(field_mapping.get(value)))
            cols.append(idx-1)
    logger.debug(values)
    logger.debug(cols)
            
    X = sp.csr_matrix((values, ([0]*len(values), cols)))
    m = X.shape[0]
    X_2 = X[np.array(range(0,m)),:]
    logger.debug(X_2)
    
    binary_model_1 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_1_KEY])
    binary_model_2 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_2_KEY])
    binary_model_3 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_3_KEY])
    binary_model_4 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_4_KEY])
    binary_model_5 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_5_KEY])
    binary_model_6 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_6_KEY])
    binary_model_7 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_7_KEY])
    binary_model_8 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_8_KEY])
    binary_model_9 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_9_KEY])
    binary_model_10 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_10_KEY])
    binary_model_11 = copy.deepcopy(app.config['models'][const.REJECT_CODE_MODEL_11_KEY])
    # print('id for rf_util in compute is - ' + str(id(rf_multi)))
    # rf_multi = copy.deepcopy(rf_multi)
    # print('id for rf_util in compute after copy is - ' + str(id(rf_multi)))
    pred_test_1 = binary_model_1.predict_proba(X_2)[:, -1]
    # print(type(pred_test_1))
    pred_test_2 = binary_model_2.predict_proba(X_2)[:, -1]
    pred_test_3 = binary_model_3.predict_proba(X_2)[:, -1]
    pred_test_4 = binary_model_4.predict_proba(X_2)[:, -1]
    pred_test_5 = binary_model_5.predict_proba(X_2)[:, -1]
    pred_test_6 = binary_model_6.predict_proba(X_2)[:, -1]
    pred_test_7 = binary_model_7.predict_proba(X_2)[:, -1]
    pred_test_8 = binary_model_8.predict_proba(X_2)[:, -1]
    pred_test_9 = binary_model_9.predict_proba(X_2)[:, -1]
    pred_test_10 = binary_model_10.predict_proba(X_2)[:, -1]
    pred_test_11 = binary_model_11.predict_proba(X_2)[:, -1]

    test_row_1 = {}
    test_row_1['50110_score'] = pred_test_1
    test_row_1['51480_score'] = pred_test_2
    test_row_1['HDJA1_score'] = pred_test_3
    test_row_1['21640_score'] = pred_test_4
    test_row_1['01030_score'] = pred_test_5
    test_row_1['51420_score'] = pred_test_6
    test_row_1['11250_score'] = pred_test_7
    test_row_1['PREFX_score'] = pred_test_8
    test_row_1['MCR00_score'] = pred_test_9
    test_row_1['G2400_score'] = pred_test_10
    test_row_1['21410_score'] = pred_test_11

    '''
    ['01030' '10790' '10830' '11060' '11100' '11120' '11250' '11260' '11280'
    '11320' '12010' '12170' '12230' '12240' '12380' '12390' '12520' '12690'
    '12790' '12820' '12840' '12850' '12880' '12900' '13510' '14120' '14200'
    '15270' '17400' '17550' '17580' '20050' '2010M' '20200' '20250' '2025F'
    '20520' '20540' '21400' '21410' '21640' '21910' '21920' '23100' '24090'
    '24160' '24230' '24250' '24610' '24680' '24740' '24772' '25020' '32050'
    '32330' '40230' '50040' '50060' '50110' '50220' '50760' '51420' '51440'
    '51450' '51480' '51530' '51570' '51610' '75010' '85280' '90710' '9COB0'
    'ANLAB' 'ASH00' 'AUOHO' 'CLMCO' 'CLMRO' 'CXTB0' 'CXTH0' 'D0706' 'D1308'
    'G2400' 'GAASH' 'HDJA1' 'HDJAA' 'LABP0' 'LABX0' 'M9000' 'M9990' 'M9999'
    'MCDJ0' 'MCJ00' 'MCJS0' 'MCR00' 'NYPND' 'PAIM0' 'PREFX' 'S0002' 'SCC37'
    'WCQ02']
    '''

    df_test = pd.DataFrame(test_row_1)
    # print(df_test)
    df_predict_top3 = df_test.apply(lambda x: pd.Series(x.nlargest(3).index.values).str.split('_').str.get(0), axis=1)
    # print(df_predict_top3)
    df_pred_score_top3 = df_test.apply(lambda x: pd.Series(x.nlargest(3).values), axis=1)
    # print(df_pred_score_top3)

    '''
    df_predict_top3 = test_row.iloc[:,2:8].apply(lambda x: pd.Series(x.nlargest(3).index.values).str.split('_').str.get(0), axis=1)
    df_pred_score_top3 = test_row.iloc[:,2:8].apply(lambda x: pd.Series(x.nlargest(3).values), axis=1)
    '''

    test_row['top_1_reason'] = df_predict_top3.iloc[:, 0].to_dict()[0]
    test_row['top_1_score'] = df_pred_score_top3.iloc[:, 0].to_dict()[0]
    test_row['top_2_reason'] = df_predict_top3.iloc[:, 1].to_dict()[0]
    test_row['top_2_score'] = df_pred_score_top3.iloc[:, 1].to_dict()[0]
    test_row['top_3_reason'] = df_predict_top3.iloc[:, 2].to_dict()[0]
    test_row['top_3_score'] = df_pred_score_top3.iloc[:, 2].to_dict()[0]
    # test_row.drop(labels=['50110_score','51480_score','HDJA1_score','21640_score','01030_score','51420_score','11250_score','PREFX_score','MCR00_score','G2400_score','21410_score'],axis=1,inplace=True)
    
    # logger.info(test_row)
    del binary_model_1
    del binary_model_2
    del binary_model_3
    del binary_model_4
    del binary_model_5
    del binary_model_6
    del binary_model_7
    del binary_model_8
    del binary_model_9
    del binary_model_10
    del binary_model_11
    
    return test_row