import os


def flat_dict_values(dict):
    dict_keys = [*dict.values()]
    dict_keys = [item for sublist in dict_keys for item in sublist]
    return dict_keys


sample = False

DATA_VERSION = '2019-01-17'
# PATH = '/Users/Af81392/PycharmProjects/cognitive-claims/'
PATH = os.path.dirname(os.path.realpath(__file__))
PATH = os.path.split(PATH)[0] + '/'
RAW_DIR = PATH + 'input/' + DATA_VERSION
CONN = RAW_DIR
MODEL_DATA_DIR = PATH + 'models/Model_Data'
KEY = ['KEY_CHK_DCN_NBR','CURNT_QUE_UNIT_NBR']
PROCESSED_DIR = PATH + 'output/Processed_Data'
PREPROCESS_DIR = PATH + 'output/preprocess'
LOW_FREQ_THRESH = 0
MODEL_REJECT_CODES = PATH + '/models/ltr/LTR_Reject_codes_07-2018-12-2018_col_dict.csv'
MODEL_OTHER_REASONS = PATH + '/models/ltr/LTR_Other_reasons_07_2018_12_2018_col_dict.csv'
OUTPUT_FOLDER = PATH + 'output/' + DATA_VERSION
LOGGING_CONFIG = PATH + 'config/logging.conf'

REJECT_CODE_MODEL_1_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_50110_final_model_new_data.sav'#50110
REJECT_CODE_MODEL_2_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_51480_final_model_new_data.sav'#51480
REJECT_CODE_MODEL_3_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_HDJA1_final_model_new_data.sav'#HDJA1
REJECT_CODE_MODEL_4_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_21640_final_model_new_data.sav'#21640
REJECT_CODE_MODEL_5_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_01030_final_model_new_data.sav'#01030
REJECT_CODE_MODEL_6_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_51420_final_model_new_data.sav'#51420
REJECT_CODE_MODEL_7_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_11250_final_model_new_data.sav'#11250
REJECT_CODE_MODEL_8_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_PREFX_final_model_new_data.sav'#PREFX
REJECT_CODE_MODEL_9_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_MCR00_final_model_new_data.sav'#MCR00
REJECT_CODE_MODEL_10_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_G2400_final_model_new_data.sav'#G2400
REJECT_CODE_MODEL_11_PATH = PATH + '/models/ltr/LTR_Other_reasons_xg_boost_21410_final_model_new_data.sav'#21410

UM_MODEL_PATH = PATH + '/models/um/201806_201810_date_RP_noDiagPlSRVCoverlap_gradient_boost_model.sav'

REJECT_CODE_MODEL_1_KEY = 'REJECT_CODE_MODEL_1'
REJECT_CODE_MODEL_2_KEY = 'REJECT_CODE_MODEL_2'
REJECT_CODE_MODEL_3_KEY = 'REJECT_CODE_MODEL_3'
REJECT_CODE_MODEL_4_KEY = 'REJECT_CODE_MODEL_4'
REJECT_CODE_MODEL_5_KEY = 'REJECT_CODE_MODEL_5'
REJECT_CODE_MODEL_6_KEY = 'REJECT_CODE_MODEL_6'
REJECT_CODE_MODEL_7_KEY = 'REJECT_CODE_MODEL_7'
REJECT_CODE_MODEL_8_KEY = 'REJECT_CODE_MODEL_8'
REJECT_CODE_MODEL_9_KEY = 'REJECT_CODE_MODEL_9'
REJECT_CODE_MODEL_10_KEY = 'REJECT_CODE_MODEL_10'
REJECT_CODE_MODEL_11_KEY = 'REJECT_CODE_MODEL_11'

UM_MODEL_KEY = 'UM_MODEL'

MODEL_NAME_BINARY = PATH + '/models/LTR_Other_reasons_xg_boost_depth_10_data_X_train.sav'

DB_PATH = PATH + '/data/cogx.db'

HEADER_DICT = {'ICD': ['ICD_A_CD', 'ICD_B_CD', 'ICD_C_CD', 'ICD_D_CD', 'ICD_E_CD','ICD_OTHR_1_CD','ICD_OTHR_2_CD','ICD_OTHR_3_CD','ICD_OTHR_4_CD','ICD_OTHR_5_CD'],
               'ATCHMNT_IND': ['ATCHMNT_IND'],
               'PROV_IND': ['PROV_IND'],
               'PROV_ZIP_5_CD': ['PROV_ZIP_5_CD'],
               'PROV_ZIP_4_CD': ['PROV_ZIP_4_CD'],
               'PROD_NTWK' :['PROD_NTWK'],
               'SBSCRBR_ZIP_5_CD': ['SBSCRBR_ZIP_5_CD'],
               'SBSCRBR_ZIP_4_CD': ['SBSCRBR_ZIP_4_CD'],
               'CLM_TYPE_CD': ['CLM_TYPE_CD'],
               'EDI_CLM_FLNG_IND': ['EDI_CLM_FLNG_IND'],
               'FULL_MNGMNT_BU_CD' : ['FULL_MNGMNT_BU_CD'],
               'MEMBER_SSN': ['MEMBER_SSN'],
               'PROV_TAX_ID': ['PROV_TAX_ID'],
               'PRVDR_STATUS':['PRVDR_STATUS'],
               'MK_FUND_TYPE_CD' : ['MK_FUND_TYPE_CD'],
               'CLAIM_TYPE':['CLAIM_TYPE'],
               'MBU_CD': ['MBU_CD'],
               'SRVC_TYPE_CURNT_CD': ['SRVC_TYPE_CURNT_CD'],
               'SRVC_TYPE_ORGNL_CD': ['SRVC_TYPE_ORGNL_CD'],
               'TAX_LIC_SPLTYCDE': ['TAX_LIC_SPLTYCDE'],
               'TYPE_OF_BILL_CD': ['TYPE_OF_BILL_CD'],
               'CLM_FILE_COB_IND': ['CLM_FILE_COB_IND'],
               'BILLG_TXNMY_CD': ['BILLG_TXNMY_CD'],
               'RNDRG_TXNMY_CD': ['RNDRG_TXNMY_CD'],
               'RNDRG_TAX_ID': ['RNDRG_TAX_ID']}

HEADER_NUM_COLS = ['TOTL_CHRG_AMT', 'COB_BSIC_PAYMNT_AMT', 'COB_MM_PAYMNT_AMT', 'COB_SGMNT_CNT', 'MEDCR_CNT',
                   'PAT_AGE_NBR','MM_PAY_AMT','BSIC_PAYMNT_AMT']

DETAIL_DICT = {'HCPCS_MDFR_CD': ['HCPCS_MDFR_CD'],
               'MODIFIER_CD': ['PROC_MDFR_CD', 'MDFR_1_CD', 'MDFR_2_CD', 'MDFR_3_CD'],
               'PROC_CD': ['PROC_CD'],
               'HCPCS_CD': ['HCPCS_CD'],
               'MBR_CNTRCT_TYPE_CD': ['MBR_CNTRCT_TYPE_CD'],
               'POT_CD': ['POT_CD'],
               'TRTMNT_TYPE_CD': ['TRTMNT_TYPE_CD'],
               'MBR_CNTRCT_CVRG_CD': ['MBR_CNTRCT_CVRG_CD'],
               'MBR_CNTRCT_CD': ['MBR_CNTRCT_CD'],
               'PN_CD': ['PN_CD'],
               'TOS_TYPE_CD': ['TOS_TYPE_CD']}

DETAIL_NUM_COLS = ['DTL_LINE_NBR', 'BILLD_CHRGD_AMT', 'MEDCRB_APRVD_AMT', 'MEDCRB_PAID_AMT', 'MEDCRB_COINSRN_AMT',
                   'MEDCRB_DDCTBL_AMT']

DETAIL_DATE_COLS = ['BNFT_YEAR_CNTRCT_EFCTV_DT', 'BNFT_YEAR_CNTRCT_REV_DT', 'MBR_CNTRCT_EFCTV_DT', 'MBR_CNTRCT_END_DT',
                    'SRVC_FROM_DT', 'SRVC_TO_DT']

EDIT_DICT = {'ERR_CD': ['ERR_1_CD', 'ERR_2_CD', 'ERR_3_CD', 'ERR_4_CD', 'ERR_5_CD', 'ERR_6_CD', 'ERR_7_CD', 'ERR_8_CD',
                        'ERR_9_CD', 'ERR_10_CD', 'ERR_11_CD', 'ERR_12_CD', 'ERR_13_CD']}

EDIT_KEEP_COLS = ['KEY_CHK_DCN_NBR', 'CURNT_QUE_UNIT_NBR'] + flat_dict_values(EDIT_DICT)

