import pickle
import constants as const
import csv

REJECT_CODE_MODEL_1 = pickle.load(open(const.REJECT_CODE_MODEL_1_PATH, 'rb'))
REJECT_CODE_MODEL_2 = pickle.load(open(const.REJECT_CODE_MODEL_2_PATH, 'rb'))
REJECT_CODE_MODEL_3 = pickle.load(open(const.REJECT_CODE_MODEL_3_PATH, 'rb'))
REJECT_CODE_MODEL_4 = pickle.load(open(const.REJECT_CODE_MODEL_4_PATH, 'rb'))
REJECT_CODE_MODEL_5 = pickle.load(open(const.REJECT_CODE_MODEL_5_PATH, 'rb'))
REJECT_CODE_MODEL_6 = pickle.load(open(const.REJECT_CODE_MODEL_6_PATH, 'rb'))
REJECT_CODE_MODEL_7 = pickle.load(open(const.REJECT_CODE_MODEL_7_PATH, 'rb'))
REJECT_CODE_MODEL_8 = pickle.load(open(const.REJECT_CODE_MODEL_8_PATH, 'rb'))
REJECT_CODE_MODEL_9 = pickle.load(open(const.REJECT_CODE_MODEL_9_PATH, 'rb'))
REJECT_CODE_MODEL_10 = pickle.load(open(const.REJECT_CODE_MODEL_10_PATH, 'rb'))
REJECT_CODE_MODEL_11 = pickle.load(open(const.REJECT_CODE_MODEL_11_PATH, 'rb'))
# rf_binary = pickle.load(open(const.MODEL_NAME_BINARY, 'rb'))

UM_MODEL = pickle.load(open(const.UM_MODEL_PATH, 'rb'))

models = {}

models[const.REJECT_CODE_MODEL_1_KEY] = REJECT_CODE_MODEL_1
models[const.REJECT_CODE_MODEL_2_KEY] = REJECT_CODE_MODEL_2
models[const.REJECT_CODE_MODEL_3_KEY] = REJECT_CODE_MODEL_3
models[const.REJECT_CODE_MODEL_4_KEY] = REJECT_CODE_MODEL_4
models[const.REJECT_CODE_MODEL_5_KEY] = REJECT_CODE_MODEL_5
models[const.REJECT_CODE_MODEL_6_KEY] = REJECT_CODE_MODEL_6
models[const.REJECT_CODE_MODEL_7_KEY] = REJECT_CODE_MODEL_7
models[const.REJECT_CODE_MODEL_8_KEY] = REJECT_CODE_MODEL_8
models[const.REJECT_CODE_MODEL_9_KEY] = REJECT_CODE_MODEL_9
models[const.REJECT_CODE_MODEL_10_KEY] = REJECT_CODE_MODEL_10
models[const.REJECT_CODE_MODEL_11_KEY] = REJECT_CODE_MODEL_11
# print('id for rf_util is - ' + str(id(rf_multi)))
# models['rf_binary'] = rf_binary
# print('id for rf_binary is - ' + str(id(rf_binary)))

models[const.UM_MODEL_KEY] = UM_MODEL

reject_list = []
with open(const.MODEL_REJECT_CODES, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        reject_list.append(row[0])
    
other_list = []
with open(const.MODEL_OTHER_REASONS, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        other_list.append(row[0])

model_encodings = {}
model_encodings['reject_list'] = reject_list
model_encodings['other_list'] = other_list

