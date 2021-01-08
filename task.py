import csv
import os
from datetime import datetime


config = {
    'bank1.csv': 'bank1_csv',
    'bank2.csv': 'bank2_csv',
    'bank3.csv': 'bank3_csv',
    'banks_unified_file_name': 'banks_unified',
    'banks_unified_file_format': 'csv',
    'path_to_data_files': './data',
    'output_file_column_titles': ['timestamp', 'type', 'amount', 'from', 'to']
}


class WrongFileNameException(Exception):
    """Raised when the file name is not present in config.

    Attributes:
        file_name -- file not found in config
    """

    def __init__(self, file_name):
        self.file_name = file_name
        super().__init__()

    def __str__(self):
        return 'File name {} not found in configuration.'.format(self.file_name)


class BankCSVToCSVCommon:
    def __init__(self, parsed_file_name, specific_operations,
                 csv_writer, config):
        self.parsed_file_name = parsed_file_name
        self.specific_operations = specific_operations
        self.csv_writer = csv_writer
        self.config = config

    def read_write_data_util(self, specific_operations):
        with open(os.path.join(self.config['path_to_data_files'],
                               self.parsed_file_name)) as csv_file:
            csv_parser = csv.DictReader(csv_file)

            for row in csv_parser:
                specific_operations(row, self.csv_writer)

    def read_write_data(self):
        self.read_write_data_util(self.specific_operations)


class Bank1CSVToCSV:
    def __init__(self, parsed_file_name, csv_writer, config):
        self.common = BankCSVToCSVCommon(
            parsed_file_name, self.read_write_specific_bank1, csv_writer, config
        )

    @staticmethod
    def read_write_specific_bank1(row, bank_writer):
        dt = datetime.strptime(row['timestamp'], '%b %d %Y')
        bank_writer.writerow(
            [str(dt.date()), row['type'], row['amount'],
             row['from'], row['to']])


class Bank2CSVToCSV:
    def __init__(self, parsed_file_name, csv_writer, config):
        self.common = BankCSVToCSVCommon(
            parsed_file_name, self.read_write_specific_bank2, csv_writer, config
        )

    @staticmethod
    def read_write_specific_bank2(row, bank_writer):
        dt = datetime.strptime(row['date'], '%d-%m-%Y')
        bank_writer.writerow(
            [str(dt.date()), row['transaction'], row['amounts'],
             row['from'], row['to']])


class Bank3CSVToCSV:
    def __init__(self, parsed_file_name, csv_writer, config):
        self.common = BankCSVToCSVCommon(
            parsed_file_name, self.read_write_specific_bank3, csv_writer, config
        )

    @staticmethod
    def read_write_specific_bank3(row, bank_writer):
        dt = datetime.strptime(row['date_readable'], '%d %b %Y')
        bank_writer.writerow(
            [str(dt.date()), row['type'],
             "{:.2f}".format(int(row['euro']) + int(row['cents']) / 100),
             row['from'], row['to']])


class BankParser:
    def __init__(self, bank_factory):
        self.bank_factory = bank_factory

    def parse_bank_file(self):
        self.bank_factory.common.read_write_data()


class MainBankExecutor:
    def __init__(self, config):
        self.config = config
        self.parser = None

    def execute(self):
        data_files = os.listdir(self.config['path_to_data_files'])

        if self.config['banks_unified_file_format'] == 'csv':
            with open(self.config['banks_unified_file_name'] + '.csv', mode='w',
                      newline='') as banks_unified:
                bank_writer = csv.writer(banks_unified, delimiter=',')
                bank_writer.writerow(self.config['output_file_column_titles'])

                for file in data_files:
                    if file not in self.config:
                        raise WrongFileNameException(file)
                    if self.config[file] == 'bank1_csv':
                        self.parser = \
                            BankParser(
                                Bank1CSVToCSV(file, bank_writer, self.config)
                            )
                    elif self.config[file] == 'bank2_csv':
                        self.parser = \
                            BankParser(
                                Bank2CSVToCSV(file, bank_writer, self.config)
                            )
                    elif self.config[file] == 'bank3_csv':
                        self.parser = \
                            BankParser(
                                Bank3CSVToCSV(file, bank_writer, self.config)
                            )
                    self.parser.parse_bank_file()


if __name__ == '__main__':
    executor = MainBankExecutor(config)
    executor.execute()
