import re
import traceback
import json

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