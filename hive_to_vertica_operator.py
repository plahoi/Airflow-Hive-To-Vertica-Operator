# -*- coding: utf-8 -*-
''' Operator performs copy ORC data from hive into Vertica
'''
import os

from airflow.operators.bash_operator import BashOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

bash_cmd = """
/opt/vertica/bin/vsql -U dbadmin -h {vertica_server} {vertica_database} -c \
"{vsql_query}"
"""

# As a result compiled query should look the following way:
# /opt/vertica/bin/vsql -U dbadmin -h 127.0.0.1 DWH -c \
# "COPY vertcica_schema.vertica_table FROM 'hdfs:///apps/hive/warehouse/hive_schema.db/hive_table/day=2019-03-03/*' ORC(hive_partition_cols='day');"

class HiveToVerticaOperator(BashOperator):
    ui_color = '#aaede4'

    @apply_defaults
    def __init__(
            self,
            hive_table,
            vertica_table,
            partition_column=None,
            partition_values=None,
            vertica_server='127.0.0.1',
            vertica_database='DWH',
            *args,
            **kwargs
    ):

        partitions = self.get_partition_info(
            partition_column, partition_values)
        hive_path = self.create_path_from_hive_table(hive_table, partitions)
        hive_partition_cols = self.get_hive_partition_cols(partition_column)

        # Compile vsql query
        vsql_query = 'COPY {vertica_table} FROM \'{hive_path}\' ORC{hive_partition_cols};'
        vsql_query = vsql_query.format(
            vertica_table=vertica_table, hive_path=hive_path, hive_partition_cols=hive_partition_cols)

        # Creating vsql query call within bash
        bash_command = bash_cmd.format(
            vertica_server=vertica_server, vertica_database=vertica_database, vsql_query=vsql_query)

        super(HiveToVerticaOperator, self).__init__(
            bash_command=bash_command, *args, **kwargs)

    def create_path_from_hive_table(self, hive_table, partition):
        ''' Parse shema_name.table_name and create line with folder address in hdfs
        + if partition is available - use it
        
        Attributes:
            hive_table (str): Name of schema and table in format like schema_name.table_name
            partition (str): partition column(folder) name

        '''

        hive_table_parts = hive_table.split('.')
        path = 'hdfs:///apps/hive/warehouse/{schema_name}.db/{table_name}/{partition}'.format(
            schema_name=hive_table_parts[0],
            table_name=hive_table_parts[1],
            partition=partition
        )

        return path

    def get_partition_info(self, partition_column, partition_values):
        '''
        There are two hive data path options:

        1. hdfs:///apps/hive/warehouse/hive_schema.db/hive_table/*/* 
        - If there is no partition then take all files from folder

        2. hdfs:///apps/hive/warehouse/hive_schema.db/hive_table/day=2019-03-03/* 
        - Set partition if we have one

        Attributes:
            partition_column (str): Partition column name
            partition_values (str)

        '''

        if partition_column and partition_values:
            partition_string = '{}={}/*'
            return partition_string.format(partition_column, partition_values)
        return '*/*'

    def get_hive_partition_cols(self, partition_column):
        ''' If partition column is set then vertica should be told about it

        Attributes:
            partition_column (str): Partition column name

        '''
        if partition_column:
            return "(hive_partition_cols='{column_name}')".format(column_name=partition_column)
        return ''


class HiveToVerticaOperatorPlugin(AirflowPlugin):
    name = "hive_to_vertica_operator_plugin"
    operators = [HiveToVerticaOperator]