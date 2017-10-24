from __future__ import unicode_literals

import abc
import collections
import csv
import datetime
import os

from ..helpers import sentence_case_to_snake_case, file_or_stdout
from ..logging_io import setup_logger
from ..reporters import default_report_dir

log = setup_logger(__name__)


class BaseReporter(object):
    __metaclass__ = abc.ABCMeta

    report_type = None
    column_mappings = None
    record_class_name = None
    record_class = None
    header_row = None

    def __init__(self, output_dir=default_report_dir, output_filename=None, output_format='csv'):
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.output_format = output_format

    def create_report(self, *args, **kwargs):
        return self.write_report(self.wrap_report_records(*args, **kwargs))

    def write_report(self, report_data):
        if self.output_dir:
            if not os.path.isdir(self.output_dir):
                log.debug('Creating report directory: {}'.format(self.output_dir))
                os.mkdir(self.output_dir)
            if not self.output_filename:
                self.output_filename = '{}_Report_{}{}{}'.format(
                    os.path.join(self.output_dir, sentence_case_to_snake_case(self.report_type, capitalize=True)),
                    datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),
                    os.path.extsep,
                    self.output_format
                )
            elif not os.path.isdir(os.path.dirname(self.output_filename)):
                self.output_filename = os.path.join(self.output_dir, os.path.basename(self.output_filename))

        log.info(
            'Generating {} report{}'
            .format(
                self.report_type,
                ': {}'.format(os.path.abspath(self.output_filename))
                if self.output_filename else ''
            )
        )

        if self.output_format == 'csv':
            with file_or_stdout(self.output_filename, 'wb') as csv_file:
                csv_writer = csv.writer(csv_file, lineterminator='\n', dialect='excel')
                csv_writer.writerow(self.header_row)
                for row in report_data:
                    csv_writer.writerow([value.encode('utf-8') if hasattr(value, 'encode') else value for value in row])
        else:
            raise RuntimeError('Unsupported output format: {}'.format(self.output_format))

        log.info(
            'Successfully generated {} report{}'
            .format(
                self.report_type,
                ': {}'.format(os.path.abspath(self.output_filename))
                if self.output_filename and os.path.isfile(self.output_filename) else ''
            )
        )
        return self.output_filename

    def wrap_report_records(self, *args, **kwargs):
        records = self.generate_report_records(*args, **kwargs)
        if self.record_class:
            for record in records:
                if isinstance(record, collections.Mapping):
                    record_instance = self.record_class(**{k: record[k] for k in self.column_mappings.keys() if k in record})
                else:
                    record_instance = self.record_class(*record)
                yield record_instance
        else:
            for record in records:
                yield record

    @staticmethod
    @abc.abstractmethod
    def generate_report_records(*args, **kwargs):
        return

    @staticmethod
    def setup_subclass(column_mappings, record_class_name=None):
        if record_class_name:
            record_class = collections.namedtuple(
                record_class_name,
                column_mappings.keys()
            )

            # Set default field values to None in case they are not provided
            record_class.__new__.__defaults__ = (None,) * len(record_class._fields)
        else:
            record_class = None

        header_row = column_mappings.values()
        return record_class, header_row
