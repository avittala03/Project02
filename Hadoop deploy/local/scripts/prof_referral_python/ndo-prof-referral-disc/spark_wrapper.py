import os
import logging
import subprocess32 as subprocess
from util import read_conf_file


def call_spark_application(conf, spark_application_filename, spark_conf_filename):
    enviornment_variables = ''
    if 'enviornment_variables' in conf['pyspark']:
        enviornment_variables = ' '.join(
            ['{0}={1}'.format(key, value) for key, value in conf['pyspark']['enviornment_variables'].iteritems()])

    pyspark_bin = conf['pyspark']['pyspark_bin']

    pyspark_script = spark_application_filename + ' ' + spark_conf_filename

    options = ''
    if 'options' in conf['pyspark']:
        options = ' '.join(
            ['{0} {1}'.format(key, value) for key, value in conf['pyspark']['options'].iteritems() if key != '--conf'])

        if '--conf' in conf['pyspark']['options']:
            options += ' '
            options += ' '.join(['--conf {0}={1}'.format(key, value) for key, value in
                                 conf['pyspark']['options']['--conf'].iteritems()])

    command = ' '.join([enviornment_variables, pyspark_bin, pyspark_script, options])
    print("command = {}".format(command))
    subprocess.call(command, shell=True)

if __name__ == '__main__':

    # Greeting.
    logger = logging.getLogger('call_spark_wrapper')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info("Started Calling Referral Script...")
    logger.info(" - work dir      : {0}".format(os.getcwd()))

    # Main block.
    conf = read_conf_file("input/conf.json")
    try:
        call_spark_application(conf, './referral_pattern.py', '../conf.json')

    except Exception as e:
        # Handle general exception.
        logger.error("<ERR>: Capture unknown error exception.")
        logger.error("<ERR>:  - Type    : {0}".format(type(e)))
        logger.error("<ERR>:  - Message : {0}".format(e.message))
    finally:
        # Done.
        logger.info("Finished Calling Referral Script.")

