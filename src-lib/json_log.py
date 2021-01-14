import traceback
import logging
import socket
import sys
from datetime import datetime
import json

class JSONFormatter(logging.Formatter):
    """
    JSON formatter for python logging
    You can pass additional tags on a per message basis using the key "tags" in the extra parameter.
    eg: logger.error('hello world!', extra={"tags": ["hello=world"]})
    """

    def __init__(self, tags=None, hostname=None, fqdn=False, message_type='JSON', indent=None):
        """
        :param tags: a list of tags to add to every messages
        :hostname: force a specific hostname
        :fqdn: a boolean to use the FQDN instead of the machine's hostname
        :message_type: the message type for Logstash formatters
        :indent: indent level of the JSON output
        """
        self.message_type = message_type
        self.tags = tags if tags is not None else []
        self.extra_tags = []
        self.indent = indent

        if hostname:
            self.host = hostname
        elif fqdn:
            self.host = socket.getfqdn()
        else:
            self.host = socket.gethostname()

    
    def get_extra_fields(self, record):
        # The list contains all the attributes listed in
        # http://docs.python.org/library/logging.html#logrecord-attributes
        skip_list = [
            'asctime', 'created', 'exc_info', 'exc_text', 'filename', 'args',
            'funcName', 'id', 'levelname', 'levelno', 'lineno', 'module', 'msg',
            'msecs', 'msecs', 'message', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'thread', 'threadName', 'extra']

        if sys.version_info < (3, 0):
            easy_types = (basestring, bool, dict, float, int, list, type(None))
        else:
            easy_types = (str, bool, dict, float, int, list, type(None))

        fields = {}

        if record.args:
            fields['msg'] = record.msg

        self.extra_tags = []
        for key, value in record.__dict__.items():
            if key not in skip_list:
                if key == 'tags' and isinstance(value, list):
                    self.extra_tags = value
                elif isinstance(value, easy_types):
                    fields[key] = value
                else:
                    fields[key] = repr(value)

        return fields

    def get_debug_fields(self, record):
        if record.exc_info:
            exc_info = self.format_exception(record.exc_info)
        else:
            exc_info = record.exc_text
        return {
            'exc_info': exc_info,
            'filename': record.filename,
            'lineno': record.lineno,
        }

    @classmethod
    def format_source(cls, message_type, host, path):
        return "%s://%s/%s" % (message_type, host, path)

    @classmethod
    def format_timestamp(cls, time):
        # return datetime.utcfromtimestamp(time).isoformat() + 'Z'
        return datetime.fromtimestamp(time).isoformat()

    @classmethod
    def format_exception(cls, exc_info):
        return ''.join(traceback.format_exception(*exc_info)) if exc_info else ''

    @classmethod
    def serialize(cls, message, indent=None):
        return json.dumps(message, indent=indent)

    def format(self, record, serialize=True):
        # Create message dict        
        print(record.process)
        message = {
            'timestamp': self.format_timestamp(record.created),
            'message': record.getMessage(),
            'host': self.host,
            'filename': record.filename,
            'tags': self.tags[:],
            'level': record.levelname,
            'logger': record.name,
            'lineno': record.lineno,
            'process': record.process,
        }

        # Add extra fields
        message.update(self.get_extra_fields(record))

        # Add extra tags
        if self.extra_tags:
            message['tags'].extend(self.extra_tags)

        # If exception, add debug info
        if record.exc_info or record.exc_text:
            message.update(self.get_debug_fields(record))

        if serialize:
            return self.serialize(message, indent=self.indent)
        return message