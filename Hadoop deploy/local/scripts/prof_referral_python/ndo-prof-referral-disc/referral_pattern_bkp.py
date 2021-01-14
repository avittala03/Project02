import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from datetime import datetime
from pyspark import SparkContext, HiveContext
#from util import read_conf_file
import logging

import re
import traceback
import json

import sys

conf_json = "conf_test.json" #sys.argv[1]
csv_data = "gaz2016zcta5distance100miles.csv" #sys.argv[2]
output_log = "output_log.log" #sys.argv[3]
env = "test" #sys.argv[4]

if env.lower() == 'disc': 
    if_deployment = False
else:
    if_deployment = True

ref_logger = logging.getLogger('referral_script')

###############################################################################
# Copied from commentjson package                                             #
###############################################################################

class JSONLibraryException(Exception):
    def __init__(self, json_error=""):
        tb = traceback.format_exc()
        tb = '\n'.join(' ' * 4 + line_ for line_ in tb.split('\n'))
        message = [
            'JSON Library Exception\n',
            ('Exception thrown by JSON library (json): '
             '\033[4;37m%s\033[0m\n' % json_error),
            '%s' % tb,
        ]
        Exception.__init__(self, '\n'.join(message))




def commentjson_loads(text, **kwargs):
    regex = r'\s*(#|\/{2}).*$'
    regex_inline = r'(:?(?:\s)*([A-Za-z\d\.{}]*)|((?<=\").*\"),?)(?:\s)*(((#|(\/{2})).*)|)$'
    lines = text.split('\n')

    for index, line in enumerate(lines):
        if re.search(regex, line):
            if re.search(r'^' + regex, line, re.IGNORECASE):
                lines[index] = ""
            elif re.search(regex_inline, line):
                lines[index] = re.sub(regex_inline, r'\1', line)

    try:
        return json.loads('\n'.join(lines), **kwargs)
    except Exception, e:
        raise JSONLibraryException(e.message)


def commentjson_load(fp, **kwargs):
    try:
        return commentjson_loads(fp.read(), **kwargs)
    except Exception, e:
        raise JSONLibraryException(e.message)

###############################################################################
# read configuration to setup env                                             #
###############################################################################
def read_conf_file(conf_filename):
    """
    read Pyspark setup file, and database locations
    return: dict
    """
    try:
        open(conf_filename)
    except(OSError, IOError) as e:
        print("Failed to open {} with config file name".format(conf_filename))
    with open(conf_filename, "r") as conf_file:
        conf = commentjson_load(conf_file)
    return conf
    
    
# handle all invalid zip codes to '00000'
def blank_as_zero(x):
    return when(((trim(col(x))!="") & (trim(col(x))!='null') & (trim(col(x))!='NA')), col(x)).otherwise("00000")

# handle all invalid specialty to 'unknown'
def blank_as_unknown(x):
    return when(((trim(col(x))!="") & (trim(col(x))!='null') & (trim(col(x))!='NA')), col(x)).otherwise("unknown")

def count_not_null(c, nan_as_null=False):
    """Use conversion between boolean and integer
    - False -> 0
    - True ->  1
    """
    pred = col(c).isNotNull() & (~isnan(c) if nan_as_null else lit(True)) & (col(c) != 'null') & (col(c) != 'NA')
    return sum(pred.cast("integer")).alias(c)


def setup_pyspark(conf_filename = conf_json):
#    with open(conf_filename) as configFile:
#        conf = json.load(configFile)
    conf = read_conf_file(conf_filename)
    ref_logger.info("Setup pyspark..")
    sc = SparkContext(appName="referral_pat")
    sc.setLogLevel("ERROR")
    hc = HiveContext(sc)
    return conf, sc, hc

class RefPattern(object):

    def __init__(self, conf=None):
        """
        Constructor: setup config file
        """
        self.conf = conf
        # input source
        # MAD, dist is pyspark dataframe
        self.MAD = None
        self.dist = None
        # claim table name
        self.output_db_name = None
        self.hive_table = None
        self.out_hive = None
        self.rule_weight = {}
        self.tot_ref_rate = 0.0
        self.PCP = None
        # other data structures
        # from_npi, to_npi pairs
        # with structure list of tuples [(from_npi, to_npi), (from_npi, to_npi),...]
        self.known_ref_pair = []
        # pyspark dataframe to store known referrals
        self.mod_exp = None

    def load_data(self, hc, load_known_ref_pair=False):
        """load data"""
        self.__load_param()
        self.__load_dist_data(hc)
        self.__load_claim_data(hc, load_known_ref_pair=False)

    def __load_param(self):
        """load parameters: rule weight, PCP definition, """
        if "rule_weight" in self.conf:
            if "total" in self.conf['rule_weight']:
                self.rule_weight['total'] = self.conf['rule_weight']['total']
            if "time" in self.conf['rule_weight']:
                self.rule_weight['time'] = self.conf['rule_weight']['time']
            if "space" in self.conf['rule_weight']:
                self.rule_weight['space'] = self.conf['rule_weight']['space']
            if "diag" in self.conf['rule_weight']:
                self.rule_weight['diag'] = self.conf['rule_weight']['diag']
            if "org" in self.conf['rule_weight']:
                self.rule_weight['org'] = self.conf['rule_weight']['org']
        if "tot_ref_rate" in self.conf:
            self.tot_ref_rate = self.conf['tot_ref_rate']
        else:
            raise ValueError("No total referral rate is specified!")
        if "PCP" in self.conf:
            self.PCP = self.conf['PCP']
        else:
            raise ValueError("No PCP defined in config file..")


    def __load_dist_data(self, hc):
        """load distance between zips data from csv file to pyspark df"""
        pd_dist = pd.read_csv(csv_data)
        pd_dist['zip1'] = pd_dist.zip1.astype(str)
        pd_dist['zip1'] = pd_dist['zip1'].str.zfill(5)
        pd_dist['zip2'] = pd_dist.zip2.astype(str)
        pd_dist['zip2'] = pd_dist['zip2'].str.zfill(5)
        self.dist = hc.createDataFrame(pd_dist)
        ref_logger.info("Zip distance data loaded into pyspark dataframe..")

    def __rename_MAD(self):
        """ rename MAD columns to be consistent with the mapping file located at
            https://confluence.am.com/display/NDO/NDO+-+Optimized+Data+Set+for+API
        """
        self.MAD = self.MAD.withColumnRenamed("CLM_SRVC_FRST_DT", "clm_srvc_first_dt")
        self.MAD = self.MAD.withColumnRenamed("TOTL_BILLD_AMT", "tot_bil")
        self.MAD = self.MAD.withColumnRenamed("TOTL_ALWD_AMT", "tot_alow")
        self.MAD = self.MAD.withColumnRenamed("TOTL_PAID_AMT", "tot_paid")
        self.MAD = self.MAD.withColumnRenamed("ST_CD", "pcr_cntrctd_st_desc")
        self.MAD = self.MAD.withColumnRenamed("LOB_ID", "pcr_lob_desc")
        self.MAD = self.MAD.withColumnRenamed("PROD_ID", "pcr_prod_desc")
        self.MAD = self.MAD.withColumnRenamed("MBR_ZIP_CD", "mem_zip")
        self.MAD = self.MAD.withColumnRenamed("RFRL_NPI", "ref_npi")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_SPCLTY_CD", "ref_npi_ep_spclty_cd")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_SPCLTY_DESC", "ref_npi_ep_spclty_desc")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_ZIP_CD", "ref_npi_prov_zip")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_TXNMY_CD", "ref_npi_txnmy_cd")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_NPPES_PROV_NM", "ref_npi_nppes_provider_name")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_TAX_ID", "ref_tax")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_TAX_NM", "ref_ep_prov_name")
        self.MAD = self.MAD.withColumnRenamed("RFRL_PROV_TAX_ZIP_CD", "ref_tax_prov_zip")
        self.MAD = self.MAD.withColumnRenamed("BILLG_NPI", "bil_npi")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_SPCLTY_CD", "bil_npi_ep_spclty_cd")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_SPCLTY_DESC", "bil_npi_ep_spclty_desc")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_ZIP_CD", "bil_npi_prov_zip")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_TXNMY_CD", "bil_npi_txnmy_cd")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_NPPES_PROV_NM", "bil_npi_nppes_provider_name")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_TAX_ID", "bil_tax")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_TAX_NM", "bil_ep_prov_name")
        self.MAD = self.MAD.withColumnRenamed("BILLG_PROV_TAX_ZIP_CD", "bil_tax_prov_zip")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_NPI", "ren_npi")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_SPCLTY_CD", "ren_npi_ep_spclty_cd")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_SPCLTY_DESC", "ren_npi_ep_spclty_desc")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_ZIP_CD", "ren_npi_prov_zip")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_TXNMY_CD", "ren_npi_txnmy_cd")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_NPPES_PROV_NM", "ren_npi_nppes_provider_name")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_TAX_ID", "ren_tax")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_TAX_NM", "ren_ep_prov_name")
        self.MAD = self.MAD.withColumnRenamed("RNDRG_PROV_TAX_ZIP_CD", "ren_tax_prov_zip")


    def __load_claim_data(self, hc, load_known_ref_pair=False):
        """
        load claim level data from Hive table
        load known_ref_pair
        input: config file
        """
        if "input" in self.conf:
            if "claim" in self.conf['input']:
                self.hive_table = self.conf['input']['claim']
            else:
                raise ValueError("No claim data location specified..")
            if "distance" in self.conf['input']:
                self.hive_distance = self.conf['input']['distance']
            else:
                raise ValueError("No zip code distance hive location specified..")
        else:
            raise ValueError("No input location specified..")
        
        if "intermediate_db_name" in self.conf:
            self.intermediate_db_name = self.conf['intermediate_db_name']
        else:
            raise ValueError("No intermediate db name specified..")

        if "output_db_name" in self.conf:
            self.output_db_name = self.conf['output_db_name']
        else:
            raise ValueError("No output db name specified..")

        if "output_table_name" in self.conf:
            self.out_hive = self.output_db_name + '.' + self.conf['output_table_name']
        else:
            raise ValueError("No final output table name specified..")

        print("MAD table name = {}".format(self.hive_table))
        query = """select a.* 
            from
            {MAD} a
            """.format(MAD=self.hive_table)
        self.MAD = hc.sql(query)

        # Change columns names of MAD for production
        if if_deployment:
            self.__rename_MAD()

        # load known_ref_pair, with structure {state: [[from_npi, to_npi], ...]}
        # TODO: to avoid out of memory after collect, is there any better way to cut the data?
        if load_known_ref_pair:
            query = """select distinct trim(pcr_cntrctd_st_desc) as state
                            from
                            {MAD} a
                            """.format(MAD=self.hive_table)
            df_st = hc.sql(query)
            pdf_st = df_st.rdd.flatMap(lambda x: x).collect()
            ref_pair = dict()
            for state in pdf_st:
                print("Loading state {}..".format(state))
                query = """select distinct ref_npi,
                        ren_npi
                        from
                        {MAD} a
                        where
                        ref_npi <> 'NA' and ref_npi <> 'null'
                        and 
                        ren_npi <> 'NA' and ren_npi <> 'null'
                        and
                        upper(pcr_cntrctd_st_desc) = upper("{state}")
                        """.format(MAD=self.hive_table, state=state)
                df = hc.sql(query)
                pdf = df.rdd.map(lambda row: (row[0], row[1])).collect()
                ref_pair[state] = pdf
            # combine ref_pair into list of of [from, to] pairs, delete state information
            ref_pair_set = set()
            for key in ref_pair:
                ref_pair_set.update(ref_pair[key])
            # ref pair as list of tuples
            self.known_ref_pair = list(ref_pair_set)
            print("Distinct known ref pairs = {}".format(len(self.known_ref_pair)))
            print("Data loading completed!")
            ref_logger.info("Data loading completed")

    def impute_MAD(self, ):
        # Fill in NPI zip codes and mbr zip code to '00000' for invalid value
        self.MAD.na.fill({'mem_zip': '00000', 'ref_npi_prov_zip':'00000', 'bil_npi_prov_zip':'00000', 'ren_npi_prov_zip':'00000'})
        self.MAD = self.MAD.withColumn('mem_zip', blank_as_zero('mem_zip'))
        self.MAD = self.MAD.withColumn('ref_npi_prov_zip', blank_as_zero('ref_npi_prov_zip'))
        self.MAD = self.MAD.withColumn('bil_npi_prov_zip', blank_as_zero('bil_npi_prov_zip'))
        self.MAD = self.MAD.withColumn('ren_npi_prov_zip', blank_as_zero('ren_npi_prov_zip'))

        # Fill in unknown specialty as 'unknown'
        self.MAD = self.MAD.withColumn('ref_npi_ep_spclty_desc', blank_as_unknown('ref_npi_ep_spclty_desc'))
        self.MAD = self.MAD.withColumn('bil_npi_ep_spclty_desc', blank_as_unknown('bil_npi_ep_spclty_desc'))
        self.MAD = self.MAD.withColumn('ren_npi_ep_spclty_desc', blank_as_unknown('ren_npi_ep_spclty_desc'))

        # Add to self.MAD new columns coming from the first two digits of zip
        self.MAD = self.MAD.withColumn('mbr_zip2', self.MAD.mem_zip.substr(1, 2))
        self.MAD = self.MAD.withColumn('ref_npi_zip2', self.MAD.ref_npi_prov_zip.substr(1, 2))
        self.MAD = self.MAD.withColumn('bil_npi_zip2', self.MAD.bil_npi_prov_zip.substr(1, 2))
        self.MAD = self.MAD.withColumn('ren_npi_zip2', self.MAD.ren_npi_prov_zip.substr(1, 2))
        # Change string to date format
        # This function converts the string cell into a date:
        func2date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
        self.MAD = self.MAD.withColumn('clm_srvc_first_dt', func2date(col('clm_srvc_first_dt')))
        self.MAD = self.MAD.withColumn('clm_srvc_end_dt', func2date(col('clm_srvc_end_dt')))

        #self.MAD.show()
        self.MAD.printSchema()
        print("MAD imputation completed!")
        ref_logger.info("MAD imputation completed!")


    def claim_data_stats(self, hc, check_missing = True):
        """
        Data exploration and output statistics
        :param df: Pyspark dataframe
        :return: None
        """
        print("Number of records = {}".format(self.MAD.count()))
        print("Columns = {}".format(self.MAD.columns))
        self.MAD.printSchema()

        # 1. checking missing
        # count null or nan: https://stackoverflow.com/questions/33900726/
        # count-number-of-non-nan-entries-in-each-column-of-spark-dataframe-with-pyspark/33901312
        df = self.MAD.select(*self.MAD.columns)
        df = df.withColumn('tot_bil', df['tot_bil'].cast("string"))
        df = df.withColumn('tot_alow', df['tot_alow'].cast("string"))
        df = df.withColumn('tot_paid', df['tot_paid'].cast("string"))
        df = df.withColumn('mcid', df['mcid'].cast("string"))
        df = df.withColumn('clm_srvc_first_dt', df['clm_srvc_first_dt'].cast("string"))
        df = df.withColumn('clm_srvc_end_dt', df['clm_srvc_end_dt'].cast("string"))
        if check_missing:
            exprs = [(count_not_null(c, True) / count("*")).alias(c) for c in df.columns]
            print("Percentage of Non-missing and Non-missing count:")
            non_missing = df.agg(*exprs)
            non_missing1 = non_missing.toPandas()
            non_missing_cnt = df.agg(*[count_not_null(c, True) for c in df.columns]).toPandas()
            non_missing.show()
            print(non_missing1)
            print(non_missing_cnt)

        # 2. check prod and (state, lob, prod) level counts
        prod_cnt = df.groupBy('pcr_prod_desc').count().sort(desc("count")).toPandas()
        comb_cnt = df.groupBy('pcr_cntrctd_st_desc', 'pcr_lob_desc', 'pcr_prod_desc').count().sort(desc("count")).toPandas()
        with pd.option_context('display.max_rows', None, 'display.max_columns', 10):
            print(prod_cnt)
            print(comb_cnt)

        # 3. check explicit referral count for (1) at PROD level (2) at (state, lob, PROD) level
        query = """select 
        pcr_prod_desc,
        count(ref_npi) as ref_cnt
        from {MAD}
        WHERE 
        ref_npi <> 'NA' and ref_npi <> 'null'
        group BY 
        pcr_prod_desc
        order by
        ref_cnt DESC 
        """.format(MAD=self.hive_table)
        ref_by_prod = hc.sql(query)
        ref_by_prod = ref_by_prod.toPandas()
        print(ref_by_prod['pcr_prod_desc'][0], ref_by_prod['ref_cnt'][0])
        print(ref_by_prod)

        query = """select
            pcr_cntrctd_st_desc,
            pcr_lob_desc,
            pcr_prod_desc,
            count(ref_npi) as ref_cnt
            from {MAD}
            WHERE
            ref_npi <> 'NA' and ref_npi <> 'null'
            group BY
            pcr_cntrctd_st_desc,
            pcr_lob_desc,
            pcr_prod_desc
            order by ref_cnt DESC
            """.format(MAD=self.hive_table)
        ref_by_comb = hc.sql(query)
        ref_by_comb = ref_by_comb.toPandas()
        with pd.option_context('display.max_rows', None, 'display.max_columns', 10):
            print(ref_by_comb)

        # 4. Compute known referral rate
        prod_ref_rate = {}
        for index, row1 in prod_cnt.iterrows():
            prod = row1['pcr_prod_desc']
            found = False
            for index, row2 in ref_by_prod.iterrows():
                if row2['pcr_prod_desc'] == prod:
                    prod_ref_rate[prod] = row2['ref_cnt'] * 1.0 / row1['count']
                    found = True
                    break
            if not found:
                prod_ref_rate[prod] = 0.0
        print("Known referral rate by PROD:")
        for k, v in prod_ref_rate.iteritems():
            print("{0}: {1}".format(k, v))
        comb_ref_rate = {}
        for index, row1 in comb_cnt.iterrows():
            st = row1['pcr_cntrctd_st_desc']
            lob = row1['pcr_lob_desc']
            prod = row1['pcr_prod_desc']
            key = st + '_' + lob + '_' + prod
            found = False
            for index, row2 in ref_by_comb.iterrows():
                if row2['pcr_cntrctd_st_desc'] == st and row2['pcr_lob_desc'] == lob and row2['pcr_prod_desc'] == prod:
                    comb_ref_rate[key] = row2['ref_cnt']*1.0/row1['count']
                    found = True
                    break
            if not found:
                comb_ref_rate[key] = 0.0
        print("Known referral rate by State, LOB, PROD:")
        for k, v in comb_ref_rate.iteritems():
            print("{0} {1} {2}: {3}".format(k.split('_')[0], k.split('_')[1], k.split('_')[2], v))

        # 5. TODO: Compare billing NPI vs Rendering NPI
        # 6. TODO: Check if NPI is there, the missing rate for specialty


    def validate_referral2(self, hc, space_cond=True):
        """
        return total count at claim level, and valid count at claim level
        :return:
        """
        # Step1: Select all claims that have a from NPI, and extract from_NPI's necessary information
        #        Note that all these from_NPI should have a corresponding claim having it as a rendering NPI
        #        Note that we may need to skip first several months, since a precedence claim might not be pulled
        #        Note that it might happen A refers to B, then MCID keep visiting B after that, with record A->B;
        #        This situation is checked to have minimal impact
        # TODO: after spark version 1.6.2, switch to createOrReplaceTempView
        self.MAD.createOrReplaceTempView("MAD")
        query = """select distinct 
                    clm_adjstmnt_key as from_clm_key
                   , diag_1_cd as from_diag
                   , mcid as from_mcid
                   , mem_zip as from_mbr_zip
                   , ref_npi as from_npi
                   , ref_npi_ep_spclty_desc as from_spclty
                   , ref_npi_prov_zip as from_ref_zip
                   , ren_npi_prov_zip as from_ren_zip
                   , ref_npi_zip2 as from_ref_zip2
                   , ren_npi_zip2 as from_ren_zip2
                   , ref_npi_txnmy_cd as from_txnmy
                   , ref_tax as from_tax
                   , pcr_cntrctd_st_desc as from_state
                   , pcr_lob_desc as from_lob
                   , pcr_prod_desc as from_prod
                   , icd_ctgry_1_txt as from_ctgry_1
                   , tot_alow as from_alow
                   , clm_srvc_first_dt as from_start
                   , clm_srvc_end_dt as from_end
                   , bil_tax as from_bill_tax
                   from MAD 
                   where
                   ref_npi <> 'NA' and ref_npi <> 'null'
                   and 
                   clm_srvc_first_dt >= '2017-04-01'
                """
        df_from = hc.sql(query)
        df_from.show()
        print("count on df_from={}".format(df_from.count()))
        print("Step 1 done..")

        # Step2: df_from left join with MAD, so that each row in df_from, can be associated with multiple claims,
        #        where each claim having from_npi as rendering NPI, at the same time, satisfying time, space, etc.
        #        conditions
        cond0 = [df_from.from_mcid == self.MAD.mcid,
                 df_from.from_npi == self.MAD.ren_npi,
                 #df_from.from_start >= self.MAD.clm_srvc_first_dt
                 datediff(df_from.from_start, self.MAD.clm_srvc_first_dt) >= -30
                 ]

        # Add time constraint
        cond1 = [df_from.from_mcid == self.MAD.mcid,
                 df_from.from_npi == self.MAD.ren_npi,
                 df_from.from_start >= self.MAD.clm_srvc_first_dt,
                 datediff(df_from.from_start, self.MAD.clm_srvc_first_dt) <= 90
                 ]
        # Add space constraint:
        # All invalid zip are '00000'
        cond2 = [df_from.from_mcid == self.MAD.mcid,
                 df_from.from_npi == self.MAD.ren_npi,
                 df_from.from_start >= self.MAD.clm_srvc_first_dt,
                #datediff(df_from.from_start, self.MAD.clm_srvc_first_dt) <= 90,
            ((df_from.from_ref_zip2 == df_from.from_ren_zip2) |
                 (df_from.from_ref_zip == '00000') |
                 (df_from.from_ren_zip == '00000'))
                 ]
        #todo: can filter zip condtion after join
        # Add diagnostic constraint
        cond3 = [df_from.from_mcid == self.MAD.mcid,
                df_from.from_npi == self.MAD.ren_npi,
                 df_from.from_start >= self.MAD.clm_srvc_first_dt,
                 df_from.from_ctgry_1 == self.MAD.icd_ctgry_1_txt
                 ]

        # Add group tax_id
        cond4 = [df_from.from_mcid == self.MAD.mcid,
                df_from.from_npi == self.MAD.ren_npi,
                 df_from.from_start >= self.MAD.clm_srvc_first_dt,
                 df_from.from_bill_tax == self.MAD.bil_tax,
                 df_from.from_bill_tax != '',
                 df_from.from_bill_tax != 'null',
                 df_from.from_bill_tax != 'NA'
                 ]

        df_valid_tmp = df_from.join(self.MAD, cond4, how='left')
        df_valid_tmp.show()
        df_valid = df_valid_tmp.where( (col('mcid').isNotNull()) )
        print("Step 2 done..")

        # Step3: Group by the "from_" field, and count valid: if the count>=1, meaning it is a valid from_npi
        #        Group by fields should be the same as in step1
        df_cnt_tmp = df_valid.groupBy('from_clm_key').agg(count('*').alias('cnt_valid'))
        df_cnt_valid = df_cnt_tmp.where(df_cnt_tmp.cnt_valid >= 1)
        df_cnt_valid.show()
        print("Step 3 done..")

        # Step4: Obtain final counts: cnt on total known referral claims, cnt on valid known referral claims
        tot_ref_cnt = df_from.count()
        tot_valid_cnt = df_cnt_valid.count()
        print("Step 4 done..")
        print(tot_ref_cnt, tot_valid_cnt)

        # Step5: Counts by state, lob, prod, etc.

    def __create_outer_loop_df(self, hc):
        """
        pull all claims where ren_NPI is valid, and ref_npi is empty:
        For each such ren_NPI (outer loop), need to loop over claims (inner loop) to find implicit count
        :param hc: hive context
        :return: spark dataframe
        """
        self.MAD.createOrReplaceTempView("MAD")
        query = """select distinct 
                    clm_adjstmnt_key as outer_loop_clm_key
                   , diag_1_cd as outer_loop_diag
                   , mcid as outer_loop_mcid
                   , mem_zip as outer_loop_mbr_zip
                   , ren_npi as outer_loop_npi
                   , ren_npi_ep_spclty_desc as outer_loop_spclty
                   , ren_npi_prov_zip as outer_loop_zip
                   , ren_npi_zip2 as outer_loop_zip2
                   , ren_npi_txnmy_cd as outer_loop_txnmy
                   , ren_tax as outer_loop_tax
                   , pcr_cntrctd_st_desc as outer_loop_state
                   , pcr_lob_desc as outer_loop_lob
                   , pcr_prod_desc as outer_loop_prod
                   , icd_ctgry_1_txt as outer_loop_ctgry_1
                   , tot_alow as outer_loop_alow
                   , clm_srvc_first_dt as outer_loop_start
                   , clm_srvc_end_dt as outer_loop_end
                   , bil_tax as outer_loop_bil_tax
                   from MAD 
                   where
                   ren_npi <> 'NA' and ren_npi <> 'null'
                   and (ref_npi = 'NA' or ref_npi = 'null')
                """
        df_outer = hc.sql(query)
        ref_logger.info("Outer loop df created")
        return df_outer

    def __group_by_outer_loop(self, df_valid):
        """
        Group by "outer_loop_clm_key" and (from, to) pair,
        and count how many inner_loop claims (distinct MCID) can match to each outer_loop claim:
        there might be multiple inner loop claims under the same (outer_loop_row, to),
        the more the count, the stronger the relationship
        df_valid: dataframe after outer_loop_claim left join inner_loop_claim, and only maintained valid matches
        :return: pyspark dataframe
        """
        # Note: The idea is to count for EACH outer loop claim (however, it cannot avoid the case of A->B->B->B,
        # which would count 3 times finally)
        # should count just distinct MCID in the inner loop, since if it is the same person with multiple claims,
        # then should only count once: the count should be = 1, since for each outer loop claim, only 1 MCID involved
        # df_cnt_valid = df_valid.groupBy('outer_loop_clm_key',
        #                                 'outer_loop_state',
        #                                 'outer_loop_lob',
        #                                 'outer_loop_prod',
        #                                 'ren_npi',
        #                                 'ren_npi_ep_spclty_desc',
        #                                 'outer_loop_npi',
        #                                 'outer_loop_spclty',
        #                                 'outer_loop_alow') \
        #     .agg(countDistinct("mcid").alias('cnt_mcid')).orderBy('outer_loop_clm_key')
        df_cnt_valid = df_valid.select('outer_loop_clm_key',
                                         'outer_loop_state',
                                         'outer_loop_lob',
                                         'outer_loop_prod',
                                         'ren_npi',
                                         'ren_npi_ep_spclty_desc',
                                         'outer_loop_npi',
                                         'outer_loop_spclty',
                                         'outer_loop_alow').withColumn("cnt_mcid", lit(1))\
                                        .distinct()
        df_cnt_valid = df_cnt_valid.withColumnRenamed("outer_loop_state", "state").withColumnRenamed("outer_loop_lob",
                                                                                                     "lob") \
            .withColumnRenamed("outer_loop_prod", "prod").withColumnRenamed("ren_npi", "from_npi") \
            .withColumnRenamed("ren_npi_ep_spclty_desc", "from_spclty").withColumnRenamed("outer_loop_npi", "to_npi") \
            .withColumnRenamed("outer_loop_spclty", "to_spclty").withColumnRenamed("cnt_mcid", "imp_claim_cnt") \
            .withColumnRenamed("outer_loop_alow", "imp_claim_amt")
        #df_cnt_valid.show(30)
        ref_logger.info("Done __group_by_outer_loop()")
        return df_cnt_valid


    def __normalize_cnt(self, df_cnt_valid):
        """
        Normalize the count, so that the total count should equal 1 for each outer_loop_key
        normalize the amount, so that total amount should equal to amt for each outer_loop_key
        df_cnt_valid: df after count matched and valid inner loop claims and dollar amount for each outer loop claim
        :return: pyspark df
        """
        df_cnt_norm_tmp = df_cnt_valid.groupBy('outer_loop_clm_key') \
            .agg({'imp_claim_cnt': 'sum'}) \
            .withColumnRenamed('sum(imp_claim_cnt)', 'grand_cnt')
        #df_cnt_norm_tmp.show(30)
        df_cnt_norm_tmp = df_cnt_valid \
            .join(df_cnt_norm_tmp, ["outer_loop_clm_key"], how='left') \
            .orderBy('outer_loop_clm_key')

        df_cnt_norm = df_cnt_norm_tmp \
            .withColumn('imp_claim_amt',
                        df_cnt_norm_tmp.imp_claim_amt * df_cnt_norm_tmp.imp_claim_cnt / df_cnt_norm_tmp.grand_cnt) \
            .withColumn('imp_claim_cnt', df_cnt_norm_tmp.imp_claim_cnt / df_cnt_norm_tmp.grand_cnt) \
            .drop('grand_cnt')
        #df_cnt_norm.show(30)
        ref_logger.info("Done __normalize_cnt()")
        return df_cnt_norm

    def __imp_cnt_match_space(self, df_outer):
        """
        Generate implicit referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_clm_cnt, imp_clm_amt
        Rule used to match: found same MCID back in time and match first 2 zip
        :return: count table under the given SINGLE rule
        """
        # Step1: use df_outer result to left join MAD, under given condition, serve as inner loop
        # Rule: MCID can be found back in time and zip2 can be matched, cannot be the same claim
        cond = [df_outer.outer_loop_mcid == self.MAD.mcid,
                df_outer.outer_loop_start >= self.MAD.clm_srvc_first_dt,
                df_outer.outer_loop_zip2 == self.MAD.ren_npi_zip2,
                df_outer.outer_loop_npi != self.MAD.ren_npi
                ]
        df_valid_tmp = df_outer.join(self.MAD, cond, how='left')
        df_valid = df_valid_tmp.where((col('mcid').isNotNull()))\
            .where((col('ren_npi') != 'null') & (col('ren_npi') != 'NA')).orderBy('outer_loop_clm_key')

        ref_logger.info("Space Join Step Done!")

        df_cnt_valid = self.__group_by_outer_loop(df_valid)
        df_cnt_norm = self.__normalize_cnt(df_cnt_valid)
        return df_cnt_norm

    def __imp_cnt_match_space2(self, hc, df_outer):
        """
        Version2 of Space Rule: Instead of match first 2 digit zip, use pre-loaded distance data
        Note that only within 100 miles data are pre-loaded
        """
        self.dist.show(30)
        self.dist.createOrReplaceTempView("distanceTable")
        hc.sql("drop table if exists {DB}.ndo_ref_zip_dist".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_zip_dist as select * from distanceTable".format(DB=self.intermediate_db_name))

        # Step 1: Satisfy conditions except 'Space rule'
        cond = [df_outer.outer_loop_mcid == self.MAD.mcid,
                df_outer.outer_loop_start >= self.MAD.clm_srvc_first_dt,
                df_outer.outer_loop_npi != self.MAD.ren_npi
                ]
        df_valid_tmp = df_outer.join(self.MAD, cond, how='inner')
        df_valid = df_valid_tmp.where((col('ren_npi') != 'null') & (col('ren_npi') != 'NA'))
        df_valid = df_valid.select('outer_loop_clm_key',
                                         'outer_loop_state',
                                         'outer_loop_lob',
                                         'outer_loop_prod',
                                         'ren_npi',
                                         'ren_npi_ep_spclty_desc',
                                         'outer_loop_npi',
                                         'outer_loop_spclty',
                                         'outer_loop_alow',
                                         'outer_loop_zip',
                                         'ren_npi_prov_zip')
        df_valid.createOrReplaceTempView("baseTable")
        hc.sql("drop table if exists {DB}.ndo_ref_ini_space".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_ini_space as select * from baseTable".format(DB=self.intermediate_db_name))

        # Step 2: Further find those within 100 mile distance
        # A match if the two zip can be found in the same row in distance table: threshold 100 miles
        # Note that in the distance data file: (zip1, zip2) has a duplicate with (zip2, zip1) both having the same distance
        query = """ select a.*, b.*
                    from
                    baseTable a
                    inner join
                    distanceTable b
                    on
                    trim(a.outer_loop_zip) = trim(b.zip1)
                    and
                    trim(a.ren_npi_prov_zip) = trim(b.zip2)
                    """
        df_valid_dist = hc.sql(query)
        df_valid_dist = df_valid_dist.drop('zip1').drop('zip2').drop('mi_to_zcta5').orderBy('outer_loop_clm_key')

        # same zip code should be included
        df_valid = hc.sql("select * from {DB}.ndo_ref_ini_space".format(DB=self.intermediate_db_name))
        df_same_zip = df_valid.where( (col('outer_loop_zip')==col('ren_npi_prov_zip')) & (col('outer_loop_zip')!='00000'))
        df_same_zip = df_same_zip.select('outer_loop_clm_key',
                                         'outer_loop_state',
                                         'outer_loop_lob',
                                         'outer_loop_prod',
                                         'ren_npi',
                                         'ren_npi_ep_spclty_desc',
                                         'outer_loop_npi',
                                         'outer_loop_spclty',
                                         'outer_loop_alow')
        df_valid_final = df_same_zip.unionAll(df_valid_dist.select(df_same_zip.columns))
        ref_logger.info("Space Join Step Done!")

        df_cnt_valid = self.__group_by_outer_loop(df_valid_final)
        df_cnt_norm = self.__normalize_cnt(df_cnt_valid)
        return df_cnt_norm


    def __imp_cnt_match_diag(self, df_outer):
        """
        Generate implicit referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_claim_cnt, imp_claim_amt
        Rule used to match: found same MCID back in time and patient categ 1 code match
        :return: count table under the given SINGLE rule
        """
        # Step1: use df_outer result to left join MAD, under given condition, serve as inner loop
        # Rule: MCID can be found back in time and patient diag can be matched, cannot be the same claim
        cond = [df_outer.outer_loop_mcid == self.MAD.mcid,
                df_outer.outer_loop_start >= self.MAD.clm_srvc_first_dt,
                df_outer.outer_loop_ctgry_1 == self.MAD.icd_ctgry_1_txt,
                df_outer.outer_loop_npi != self.MAD.ren_npi
                ]
        df_valid_tmp = df_outer.join(self.MAD, cond, how='inner')
        df_valid = df_valid_tmp.where((col('ren_npi') != 'null') & (col('ren_npi') != 'NA')).orderBy('outer_loop_clm_key')

        ref_logger.info("Diagnose join step Done!")
        df_cnt_valid = self.__group_by_outer_loop(df_valid)
        df_cnt_norm = self.__normalize_cnt(df_cnt_valid)
        return df_cnt_norm

    def __imp_cnt_match_org(self, df_outer):
        """
        Generate implicit referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_clm_cnt, imp_clm_amt
        Rule used to match: found same MCID back in time and bill group tax_id match
        :return: count table under the given SINGLE rule
        """
        # Step1: use df_outer result to left join MAD (inner loop), under given condition, serve as inner loop
        # Rule: MCID can be found back in time and billing tax_id can be matched, cannot be the same claim
        # used bil_tax since missing rate much lower than ren_tax
        cond = [df_outer.outer_loop_mcid == self.MAD.mcid,
                df_outer.outer_loop_start >= self.MAD.clm_srvc_first_dt,
                df_outer.outer_loop_bil_tax == self.MAD.bil_tax,
                df_outer.outer_loop_npi != self.MAD.ren_npi
                ]
        df_valid_tmp = df_outer.join(self.MAD, cond, how='inner')
        df_valid = df_valid_tmp.where((col('ren_npi') != 'null') & (col('ren_npi') != 'NA')).orderBy('outer_loop_clm_key')

        ref_logger.info("Organization join step Done!")
        df_cnt_valid = self.__group_by_outer_loop(df_valid)
        df_cnt_norm = self.__normalize_cnt(df_cnt_valid)
        return df_cnt_norm

    def __imp_cnt_match_time(self, df_outer):
        """
        Generate implicit referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_clm_cnt, imp_clm_amt
        Rule used to match: found same MCID back in time and within 90 days
        :return: count table with the given SINGLE rule to match
        """
        # Step1: use outer loop claim to left join MAD, under given condition, serve as inner loop
        # Rule: MCID can be found back in time and within 90 days, cannot be the same claim
        cond = [df_outer.outer_loop_mcid == self.MAD.mcid,
                 df_outer.outer_loop_start >= self.MAD.clm_srvc_first_dt,
                 datediff(df_outer.outer_loop_start, self.MAD.clm_srvc_end_dt) <= 90,
                 df_outer.outer_loop_npi != self.MAD.ren_npi
                 ]
        df_valid_tmp = df_outer.join(self.MAD, cond, how='inner')
        #df_valid_tmp.show(30)
        df_valid = df_valid_tmp.where((col('ren_npi') != 'null') & (col('ren_npi') != 'NA')).orderBy('outer_loop_clm_key')
        #df_valid.show(30)
        ref_logger.info("Time join step Done!")
        df_cnt_valid = self.__group_by_outer_loop(df_valid)
        df_cnt_norm = self.__normalize_cnt(df_cnt_valid)
        return df_cnt_norm


    def combine_imp_cnt_across_rule(self, hc, time_wt=0.351, zip_wt=0.277, diag_wt=0.256, org_wt=0.116, overall_wt=1.0):
        """
        Rule weights read from conf.json (overall_wt set to 1.0, since tune_mod_imp() will do the job of scale
        the implicit MOD on an overall level.
        PhaseI: Add up the count from each rule using rule weight
        PhaseII: Aggregate at (state, lob, prod, from, to) level to count the number of outer loop claims
        Phase III: Delete rows with very small counts
        :return: MOD of implicit referrals
        """
        # Update rule weight according to config file, overall_wt default to 1.0, will tune in tune_mod_imp()
        time_wt = self.rule_weight['time']
        zip_wt = self.rule_weight['space']
        diag_wt = self.rule_weight['diag']
        org_wt = self.rule_weight['org']


        # Phase I: all counts are at outer loop claim level
        # generate dataframe to use for outer_loop, and pass it to each rule count
        df_outer = self.__create_outer_loop_df(hc)

        # inner loop
        time_cnt = self.__imp_cnt_match_time(df_outer)
        time_cnt.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_time_cnt".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_time_cnt as select * from tempTable".format(DB=self.intermediate_db_name))
        #print(time_cnt.groupBy().agg(sum('imp_claim_cnt')).collect())
        #print("Matched Time Row Count = {0}, Claim_Cnt = {1}, Dollar_Amt = {2}"
        #     .format(time_cnt.count(), time_cnt.groupBy().agg(sum('imp_claim_cnt')).collect()[0][0],
        #             time_cnt.groupBy().agg(sum('imp_claim_amt')).collect()[0][0] ))
        ref_logger.info("Time count table done")

        space_cnt = self.__imp_cnt_match_space2(hc, df_outer)
        space_cnt.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_space_cnt".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_space_cnt as select * from tempTable".format(DB=self.intermediate_db_name))
        #print("Matched Space Row Count = {0}, Claim_Cnt = {1}, Dollar_Amt = {2}"
        #      .format(space_cnt.count(), space_cnt.groupBy().agg(sum('imp_claim_cnt')).collect()[0][0],
        #              space_cnt.groupBy().agg(sum('imp_claim_amt')).collect()[0][0]))
        ref_logger.info("Space count table done")

        diag_cnt = self.__imp_cnt_match_diag(df_outer)
        diag_cnt.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_diag_cnt".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_diag_cnt as select * from tempTable".format(DB=self.intermediate_db_name))
        #print("Matched Diag Row Count = {0}, Claim_Cnt = {1}, Dollar_Amt = {2}"
        #      .format(diag_cnt.count(), diag_cnt.groupBy().agg(sum('imp_claim_cnt')).collect()[0][0],
        #              diag_cnt.groupBy().agg(sum('imp_claim_amt')).collect()[0][0]))
        ref_logger.info("Diagnostic count table done")

        org_cnt = self.__imp_cnt_match_org(df_outer)
        org_cnt.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_org_cnt".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_org_cnt as select * from tempTable".format(DB=self.intermediate_db_name))
        #print("Matched Org Row Count = {0}, Claim_Cnt = {1}, Dollar_Amt = {2}"
        #      .format(org_cnt.count(), org_cnt.groupBy().agg(sum('imp_claim_cnt')).collect()[0][0],
        #              org_cnt.groupBy().agg(sum('imp_claim_amt')).collect()[0][0]))
        ref_logger.info("Organization count table done")

        # To prevent from "java.lang.OutOfMemoryError: GC overhead limit exceeded"
        time_cnt = hc.sql("select * from {DB}.ndo_ref_time_cnt".format(DB=self.intermediate_db_name))
        space_cnt = hc.sql("select * from {DB}.ndo_ref_space_cnt".format(DB=self.intermediate_db_name))
        diag_cnt = hc.sql("select * from {DB}.ndo_ref_diag_cnt".format(DB=self.intermediate_db_name))
        org_cnt = hc.sql("select * from {DB}.ndo_ref_org_cnt".format(DB=self.intermediate_db_name))

        # Apply rule weight
        time_cnt = time_cnt.withColumn('imp_claim_amt', time_cnt.imp_claim_amt * time_wt * overall_wt)\
            .withColumn('imp_claim_cnt', time_cnt.imp_claim_cnt * time_wt * overall_wt)
        space_cnt = space_cnt.withColumn('imp_claim_amt', space_cnt.imp_claim_amt * zip_wt * overall_wt) \
            .withColumn('imp_claim_cnt', space_cnt.imp_claim_cnt * zip_wt * overall_wt)
        diag_cnt = diag_cnt.withColumn('imp_claim_amt', diag_cnt.imp_claim_amt * diag_wt * overall_wt) \
            .withColumn('imp_claim_cnt', diag_cnt.imp_claim_cnt * diag_wt * overall_wt)
        org_cnt = org_cnt.withColumn('imp_claim_amt', org_cnt.imp_claim_amt * org_wt * overall_wt) \
            .withColumn('imp_claim_cnt', org_cnt.imp_claim_cnt * org_wt * overall_wt)

        # union all these counts, switch to union() after spark 2.0
        df_imp_cnt = time_cnt.unionAll(space_cnt.select(time_cnt.columns))\
            .unionAll(diag_cnt.select(time_cnt.columns))\
            .unionAll(org_cnt.select(time_cnt.columns))
        #print("Total count before dropping rows = {}".format(df_imp_cnt.count()))

        # group by outer_loop_key: count for each claim level, use sum since there are multiple rules
        imp_grp = df_imp_cnt.groupBy('outer_loop_clm_key',
                                     'state',
                                     'lob',
                                     'prod',
                                     'from_npi',
                                     'from_spclty',
                                     'to_npi',
                                     'to_spclty'
                                     ).agg({'imp_claim_amt': 'sum', 'imp_claim_cnt': 'sum'})\
            .withColumnRenamed("sum(imp_claim_amt)", "imp_claim_amt")\
            .withColumnRenamed("sum(imp_claim_cnt)", "imp_claim_cnt")
        #imp_grp.show(30)
        imp_grp.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_PhaseI".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_PhaseI as select * from tempTable".format(DB=self.intermediate_db_name))
        ref_logger.info("Phase I table done")

        # Phase II: aggregate at (state, lob, prod, from, to) level from outer_loop claim level
        # to count the number of claims for each (state, lob, prod, from, to)
        mod_imp = imp_grp.groupBy('state',
                                  'lob',
                                  'prod',
                                  'from_npi',
                                  'from_spclty',
                                  'to_npi',
                                  'to_spclty'
                                  ).agg({'imp_claim_amt': 'sum', 'imp_claim_cnt': 'sum'})\
            .withColumnRenamed("sum(imp_claim_amt)", "imp_claim_amt")\
            .withColumnRenamed("sum(imp_claim_cnt)", "imp_claim_cnt")
        #mod_imp.show(30)
        print("Matched implicit referral before dropping row and tuning: Row Count = {0}, Claim_Cnt = {1}, Dollar_Amt = {2}"
              .format(mod_imp.count(), mod_imp.groupBy().agg(sum('imp_claim_cnt')).collect()[0][0],
                      mod_imp.groupBy().agg(sum('imp_claim_amt')).collect()[0][0]))

        # Phase III: drop those rows with very few implicit count:
        # Set the threshold to be 0.1: note that the total count of to_NPI is at least 1.0
        # in Phase I, if only one match condition is met, then the claim cnt is roughly 1*rule_weight['total']/4 = 0.06
        # in Phase II, count from Phase I claim level will add up: if there are multiple claims having the same from_NPI, to_NPI pair
        mod_imp = mod_imp.where(mod_imp.imp_claim_cnt >= 0.1)
        mod_imp.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_mod_imp".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_mod_imp as select * from tempTable".format(DB=self.intermediate_db_name))
        ref_logger.info("NDO_REF_MOD_IMP table created!")
        return mod_imp


    def tune_mod_imp(self, hc, tune_spclty = False, tune_PCP = True):
        """1. Adjust the output of implicit referral according to specialty to specialty probability
           2. Adjust the total rule weight based on known referral rate
           Note that this will adjust PCP to non-PCP vs non-PCP to non-PCP ratios
           PCP definition 4 from config file
           PPO and HMO are adjusted separately
        """
        ref_logger.info("Start tuning implicit MOD..")
        # Step 1: Got the metrics from known referrals
        # compute known referral rate
        known_ref = self.MAD.where((col('ref_npi')!='NA') & (col('ref_npi')!='null') )
        known_ref_cnt = known_ref.count()
        tot_cnt = self.MAD.count()
        known_ref_rate = 1.0 * known_ref_cnt / tot_cnt
        print("Tune implicit referral: known_ref_rate = {}".format(known_ref_rate))
        # compute implicit referral rate
        mod_imp = hc.sql("select * from {DB}.ndo_ref_mod_imp".format(DB=self.intermediate_db_name))
        imp_cnt = mod_imp.groupBy().sum('imp_claim_cnt').collect()[0][0]
        imp_ref_rate = 1.0 * imp_cnt/tot_cnt
        print("Tune implicit referral: current imp_ref_rate = {}".format(imp_ref_rate))
        # compute specialty to specialty percentage, i.e., if it is a referral, the prob that from specialty A to B
        spc2spc = known_ref.groupBy('ref_npi_ep_spclty_desc', 'ren_npi_ep_spclty_desc').count()
        # drop those unknown specialities, since it may be different for known ref and unknown ref
        spc2spc_cnt = spc2spc.where( (col('ref_npi_ep_spclty_desc') != 'unknown') & (col('ren_npi_ep_spclty_desc') != 'unknown') )
        #spc2spc.show(30)
        #spc2spc_cnt.show(30)
        spc2spc_sum = spc2spc_cnt.groupBy().sum("count").collect()[0][0]
        print("Total known referral claim = {}".format(spc2spc_sum))
        spc2spc_pd = spc2spc_cnt.toPandas()
        #print(spc2spc_pd)

        # Step 2: Compute number of intended implicit referral among specialties, based on step 1 counts
        target_imp_tot = tot_cnt * self.tot_ref_rate - known_ref_cnt
        print("Target implicit referral count = {}".format(target_imp_tot))
        if tune_spclty:
            target_imp_ratio = {}
            for index, row in spc2spc_pd.iterrows():
                key = (row['ref_npi_ep_spclty_desc'], row['ren_npi_ep_spclty_desc'])
                target_imp_ratio[key] = row['count'] * 1.0 / spc2spc_sum
            target_imp_ratio_list = sorted(target_imp_ratio.iteritems(), key=lambda x: x[1], reverse=True)
            print("Most possible specialty to specialty pairs:")
            for (k, v) in target_imp_ratio_list:
                if v > 0.005:
                    print("{} to {} percentage = {}".format(k[0], k[1], v))


        # Step 3.1: Tune overall implicit count and amount, and update ndo_ref_mod_imp table
        # TODO: This may need to put as the last step
        factor = target_imp_tot / imp_cnt
        print("Total rule weight adjustment factor = {}".format(factor))
        mod_imp = mod_imp.withColumn('imp_claim_cnt', col('imp_claim_cnt')*factor)\
            .withColumn('imp_claim_amt', col('imp_claim_amt')*factor)
        mod_imp_adj = mod_imp.select('*')

        # Step 3.2: Tune specialty to specilty implicit output based on known referral metrics if the ratio is out of range
        # the ratio does not depend on PROD: actually PROD may have an impact on specialty to specialty referral ratio
        if tune_spclty:
            ratio_adj = {}
            spc2spc_imp = mod_imp.groupBy('from_spclty', 'to_spclty').count()\
                .where( (col('from_spclty') != 'unknown') & (col('to_spclty') != 'unknown') )
            spc2spc_imp_pd = spc2spc_imp.toPandas()
            for index, row in spc2spc_imp_pd.iterrows():
                key = (row['from_spclty'], row['to_spclty'])
                if key not in target_imp_ratio:
                    target = 0
                else:
                    target = target_imp_tot * target_imp_ratio[key]
                # set a loose bound to allow rooms
                bound = target_imp_tot * 0.005
                if row['count'] - target > bound:
                    ratio_adj[key] = target / row['count']
                elif target - row['count'] > bound:
                    ratio_adj[key] = target / row['count']
                else:
                    ratio_adj[key] = 1.0

            print("Implicit referral adjust ratio:")
            for key, value in ratio_adj.items():
                if ratio_adj[key] != 1.0:
                    print("adjust ratio ~ {}: {}".format(key, value))

            # adjust implicit referral counts and amounts, for those specialty pairs that need adjustment

            ref_logger.info("Begin tuning specialty to specialty portion..")
            #cnt = 0
            for key, value in ratio_adj.items():
                if value != 1.0:
                    adj_rows = mod_imp.where( (col("from_spclty") == key[0]) & (col("to_spclty") == key[1]))
                    adj_rows = adj_rows.withColumn('imp_claim_cnt', col('imp_claim_cnt')*value)\
                        .withColumn('imp_claim_amt', col('imp_claim_amt')*value)
                    mod_imp = mod_imp.where( (col("from_spclty") != key[0]) | (col("to_spclty") != key[1]) )
                    mod_imp = mod_imp.unionAll(adj_rows.select(mod_imp.columns))
                    #if cnt % 10 == 0:
                    #    mod_imp.show(10)
                    #cnt += 1

        # Step 4: Tune the ratio of PCP and non-PCP: there are four possibilities:
        # PCP-> non-PCP, PCP->PCP, non-PCP->PCP, non-PCP -> non-PCP,  tune these four percentage
        if tune_PCP:
            # counts in known referral
            ref_logger.info("Start tuning PCP and non-PCP ratio..")
            pcp = "("
            for s in self.PCP:
                pcp += "'" + str(s) + "'" + ","
            pcp1 = pcp + "'unknown'" + ")"
            pcp = pcp[:-1] + ")"
            known_ref.createOrReplaceTempView("known_ref")
            query1 = """select *
                        from
                        known_ref
                        where
                        ref_npi_ep_spclty_desc in {PCP}
                        and 
                        ren_npi_ep_spclty_desc not in {PCP1}
                        """.format(PCP=pcp, PCP1=pcp1)
            known_count1 = hc.sql(query1).count()
            print("Known PCP to non-PCP count = {}".format(known_count1))
            query2 = """select *
                        from
                        known_ref
                        where
                        ref_npi_ep_spclty_desc in {PCP}
                        and 
                        ren_npi_ep_spclty_desc in {PCP}
                        """.format(PCP=pcp)
            known_count2 = hc.sql(query2).count()
            print("Known PCP to PCP count = {}".format(known_count2))
            query3 = """select *
                        from
                        known_ref
                        where
                        ref_npi_ep_spclty_desc not in {PCP1}
                        and 
                        ren_npi_ep_spclty_desc in {PCP}
                        """.format(PCP=pcp, PCP1=pcp1)
            known_count3 = hc.sql(query3).count()
            print("Known non-PCP to PCP count = {}".format(known_count3))
            query4 = """select *
                        from
                        known_ref
                        where
                        ref_npi_ep_spclty_desc not in {PCP1}
                        and 
                        ren_npi_ep_spclty_desc not in {PCP1}
                        """.format(PCP1=pcp1)
            known_count4 = hc.sql(query4).count()
            print("Known non-PCP to non-PCP count = {}".format(known_count4))
            sum_known = known_count1 + known_count2 + known_count3 + known_count4
            known_pct1 = known_count1 * 1.0 / sum_known
            known_pct2 = known_count2 * 1.0 / sum_known
            known_pct3 = known_count3 * 1.0 / sum_known
            known_pct4 = known_count4 * 1.0 / sum_known
            print("PCP to non-PCP percentage = {}".format(known_pct1))
            print("PCP to PCP percentage = {}".format(known_pct2))
            print("non-PCP to PCP percentage = {}".format(known_pct3))
            print("non-PCP to non-PCP percentage = {}".format(known_pct4))


            # tune implicit referral
            imp_pcp_tot = mod_imp.where( (col('from_spclty') != 'unknown') & (col('to_spclty') != 'unknown') )\
                .agg(sum('imp_claim_cnt')).collect()[0][0]
            print("imp_pcp_tot = {}".format(imp_pcp_tot))
            tgt_p2n = imp_pcp_tot * known_pct1
            tgt_p2p = imp_pcp_tot * known_pct2
            tgt_n2p = imp_pcp_tot * known_pct3
            tgt_n2n = imp_pcp_tot * known_pct4
            mod_imp.createOrReplaceTempView("imp_ref")
            query1 = """select *
                        from
                        imp_ref 
                        where
                        from_spclty in {PCP}
                        and 
                        to_spclty not in {PCP1}
                        """.format(PCP=pcp, PCP1=pcp1)
            imp_p2n = hc.sql(query1)
            query2 = """select *
                        from
                        imp_ref 
                        where
                        from_spclty in {PCP}
                        and 
                        to_spclty in {PCP}
                        """.format(PCP=pcp)
            imp_p2p = hc.sql(query2)
            query3 = """select *
                        from
                        imp_ref 
                        where
                        from_spclty not in {PCP1}
                        and 
                        to_spclty in {PCP}
                        """.format(PCP=pcp, PCP1=pcp1)
            imp_n2p = hc.sql(query3)
            query4 = """select *
                        from
                        imp_ref 
                        where
                        from_spclty not in {PCP1}
                        and 
                        to_spclty not in {PCP1}
                        """.format(PCP1=pcp1)
            imp_n2n = hc.sql(query4)

            factor1 = tgt_p2n * 1.0 / imp_p2n.agg(sum('imp_claim_cnt')).collect()[0][0]
            print("PCP to non-PCP adjust ratio = {}".format(factor1))
            imp_p2n = imp_p2n.withColumn('imp_claim_cnt', col('imp_claim_cnt')*factor1)\
                .withColumn('imp_claim_amt', col('imp_claim_amt')*factor1)

            factor2 = tgt_p2p * 1.0 / imp_p2p.agg(sum('imp_claim_cnt')).collect()[0][0]
            print("PCP to PCP adjust ratio = {}".format(factor2))
            imp_p2p = imp_p2p.withColumn('imp_claim_cnt', col('imp_claim_cnt') * factor2) \
                .withColumn('imp_claim_amt', col('imp_claim_amt') * factor2)

            factor3 = tgt_n2p * 1.0 / imp_n2p.agg(sum('imp_claim_cnt')).collect()[0][0]
            print("non-PCP to PCP adjust ratio = {}".format(factor3))
            imp_n2p = imp_n2p.withColumn('imp_claim_cnt', col('imp_claim_cnt') * factor3) \
                .withColumn('imp_claim_amt', col('imp_claim_amt') * factor3)

            factor4 = tgt_n2n * 1.0 / imp_n2n.agg(sum('imp_claim_cnt')).collect()[0][0]
            print("non-PCP to non-PCP adjust ratio = {}".format(factor4))
            imp_n2n = imp_n2n.withColumn('imp_claim_cnt', col('imp_claim_cnt') * factor4) \
                .withColumn('imp_claim_amt', col('imp_claim_amt') * factor4)

            mod_imp_adj = imp_p2n.unionAll(imp_p2p.select(imp_p2n.columns)).unionAll(imp_n2p.select(imp_p2n.columns))\
                .unionAll(imp_n2n.select(imp_p2n.columns))


        # drop rows with too small count
        mod_imp_adj = mod_imp_adj.where(mod_imp_adj.imp_claim_cnt >= 0.1)
        mod_imp_adj.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_mod_imp_adj".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_mod_imp_adj as select * from tempTable".format(DB=self.intermediate_db_name))
        ref_logger.info("Implicit MOD tuning completed!")
        return mod_imp_adj


    def generate_exp_cnt(self, hc):
        """
        Directly query known referral counts table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, exp_claim_cnt, exp_claim_amt
        :return:
        """
        self.MAD.createOrReplaceTempView("MAD")
        query = """select  
                    pcr_cntrctd_st_desc as state
                   , pcr_lob_desc as lob
                   , pcr_prod_desc as prod
                   , ref_npi as from_npi
                   , ref_npi_ep_spclty_desc as from_spclty
                   , ren_npi as to_npi
                   , ren_npi_ep_spclty_desc as to_spclty
                   , count(*) as exp_claim_cnt
                   , sum(tot_alow) as exp_claim_amt
                    from
                    MAD 
                    where 
                    ref_npi <> 'null' and ref_npi <> 'NA'
                    and 
                    ren_npi <> 'null' and ren_npi <> 'NA'
                    group by 
                    pcr_cntrctd_st_desc 
                   , pcr_lob_desc
                   , pcr_prod_desc 
                   , ref_npi 
                   , ref_npi_ep_spclty_desc 
                   , ren_npi 
                   , ren_npi_ep_spclty_desc
                    """
        mod_exp = hc.sql(query)
        self.mod_exp = mod_exp.select('*')
        mod_exp.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_exp".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_exp as select * from tempTable".format(DB=self.intermediate_db_name))
        ref_logger.info("exp_cnt table created")
        return mod_exp

    def generate_tot_cnt(self, hc):
        """
        Generate total referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_clm_cnt, imp_clm_amt
        :return:
        """
        self.MAD.createOrReplaceTempView("MAD")
        query = """select  
                    pcr_cntrctd_st_desc as state
                   , pcr_lob_desc as lob
                   , pcr_prod_desc as prod
                   , 'total' as from_npi
                   , 'total' as from_spclty
                   , ren_npi as to_npi
                   , ren_npi_ep_spclty_desc as to_spclty
                   , count(*) as exp_claim_cnt
                   , sum(tot_alow) as exp_claim_amt
                    from
                    MAD 
                    where 
                    ren_npi <> 'null' and ren_npi <> 'NA'
                    group by 
                    pcr_cntrctd_st_desc 
                   , pcr_lob_desc
                   , pcr_prod_desc 
                   , ren_npi 
                   , ren_npi_ep_spclty_desc
                    """
        mod_tot = hc.sql(query)
        mod_tot.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {DB}.ndo_ref_tot".format(DB=self.intermediate_db_name))
        hc.sql("create table {DB}.ndo_ref_tot as select * from tempTable".format(DB=self.intermediate_db_name))
        ref_logger.info("Done generate_tot_cnt()")
        return mod_tot


    def generate_MOD(self, hc):
        """
        Generate final referrals by generate a counting table with the following columns:
        state, lob, prod, from_npi, from_spclty, to_npi, to_spclty, imp_claim_cnt, imp_claim_amt, exp_claim_cnt, exp_claim_amt
        Combine exp_cnt, imp_cnt, tot_cnt
        :return: model output dataset
        """
        imp_cnt = self.combine_imp_cnt_across_rule(hc)
        self.generate_exp_cnt(hc)
        self.tune_mod_imp(hc)
        tot_cnt = self.generate_tot_cnt(hc)

        # To prevent GC overhead limit and out of memory, read from Hive
        imp_cnt = hc.sql("select * from {DB}.ndo_ref_mod_imp_adj".format(DB=self.intermediate_db_name))
        exp_cnt = hc.sql("select * from {DB}.ndo_ref_exp".format(DB=self.intermediate_db_name))
        tot_cnt = hc.sql("select * from {DB}.ndo_ref_tot".format(DB=self.intermediate_db_name))

        # Add to data frame missing columns with 0 value
        imp_cnt = imp_cnt.withColumn('exp_claim_amt', lit(0)).withColumn('exp_claim_cnt', lit(0))
        #imp_cnt.show(50)
        exp_cnt = exp_cnt.withColumn('imp_claim_amt', lit(0)).withColumn('imp_claim_cnt', lit(0))
        #exp_cnt.show(50)
        tot_cnt = tot_cnt.withColumn('imp_claim_amt', lit(0)).withColumn('imp_claim_cnt', lit(0))
        #tot_cnt.show(50)
        #tot_cnt.printSchema()


        # combine above three df into the final output
        comb = imp_cnt.unionAll(exp_cnt.select(imp_cnt.columns)).unionAll(tot_cnt.select(imp_cnt.columns))
        #comb.where(comb.exp_claim_cnt > 0).show(500)
        MOD = comb.groupBy('state',
                           'lob',
                           'prod',
                           'from_npi',
                           'from_spclty',
                           'to_npi',
                           'to_spclty'
                           ).agg({'exp_claim_amt':'sum', 'exp_claim_cnt':'sum', 'imp_claim_amt':'sum', 'imp_claim_cnt':'sum'})\
            .withColumnRenamed("sum(imp_claim_amt)", "imp_claim_amt")\
            .withColumnRenamed("sum(imp_claim_cnt)", "imp_claim_cnt")\
            .withColumnRenamed("sum(exp_claim_amt)", "exp_claim_amt")\
            .withColumnRenamed("sum(exp_claim_cnt)", "exp_claim_cnt")

        # Rename MOD columns to be consistent with Mapping Documentation at
        # https://confluence.am.com/display/NDO/NDO+-+Optimized+Data+Set+for+API
        MOD = MOD.withColumnRenamed("state", "ST_CD")\
            .withColumnRenamed("lob", "LOB_ID")\
            .withColumnRenamed("prod", "PROD_ID")\
            .withColumnRenamed("from_npi", "RFRL_NPI") \
            .withColumnRenamed("from_spclty", "RFRL_PROV_SPCLTY_DESC") \
            .withColumnRenamed("to_npi", "RNDRG_NPI") \
            .withColumnRenamed("to_spclty", "RNDRG_PROV_SPCLTY_DESC") \
            .withColumnRenamed("exp_claim_cnt", "EXPLCT_CLM_CNT") \
            .withColumnRenamed("exp_claim_amt", "EXPLCT_CLM_AMT") \
            .withColumnRenamed("imp_claim_cnt", "IMPLCT_CLM_CNT") \
            .withColumnRenamed("imp_claim_amt", "IMPLCT_CLM_AMT")

        # Add time stamp SNAP_YEAR_MNTH_NBR
        self.MAD.createOrReplaceTempView("MAD")
        SNAP_YEAR_MNTH_NBR = hc.sql("select distinct SNAP_YEAR_MNTH_NBR from MAD").collect()[0][0]
        MOD = MOD.withColumn("SNAP_YEAR_MNTH_NBR", lit(SNAP_YEAR_MNTH_NBR) )

        MOD.createOrReplaceTempView("tempTable")
        hc.sql("drop table if exists {output}".format(output=self.out_hive) )
        hc.sql("create table {output} as select * from tempTable".format(output=self.out_hive))
        ref_logger.info("Final MOD table created!")

        return MOD


def main():
    # Setup logger

    fh = logging.FileHandler('out_log.log', mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ref_logger.addHandler(fh)
    ref_logger.setLevel(logging.DEBUG)
    ref_logger.info("Start running main program..")

    # Setup Spark
    conf, sc, hc = setup_pyspark(conf_json)
    ref_pat = RefPattern(conf)

    # Load data
    ref_pat.load_data(hc)
    ref_pat.impute_MAD()

    # Run model
    #ref_pat.claim_data_stats(hc, check_missing=True)
    #ref_pat.validate_referral2(hc)
    #ref_pat.combine_imp_cnt_across_rule(hc)
    #ref_pat.tune_mod_imp(hc)
    #ref_pat.generate_exp_cnt(hc)
    #ref_pat.generate_tot_cnt(hc)
    MOD = ref_pat.generate_MOD(hc)
    MOD.show(100)
    ref_logger.info("Finished running __main__ part")
    sc.stop()
    if fh is not None:
        fh.flush()

if __name__ == '__main__':
    main()
