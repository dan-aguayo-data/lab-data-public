import os
import argparse
from kada_collectors.extractors.utils import load_config, get_hwm, publish_hwm, get_generic_logger
from kada_collectors.extractors.snowflake import Extractor

get_generic_logger('root') # Set to use the root logger, you can change the context accordingly or define your own logger

#print("-------Opening JSON FILE")

_type = 'snowflake'
dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, 'kada_{}_extractor_config.json'.format(_type))


#_type = 'snowflake'
#file_path = r'C:\Users\DanielAguayo\kada_credentials.json'
#dirname = os.path.dirname(file_path)
#filename = os.path.join(dirname, 'kada_{}_extractor_config.json'.format(_type))
#print("--------JSON OPENED")

parser = argparse.ArgumentParser(description='KADA Snowflake Extractor.')
parser.add_argument('--config', '-c', dest='config', default=filename, help=r'"C:\Users\DanielAguayo\PycharmProjects\KADA\kada_snowflake_extractor_config.json"') #remove path from help and add generic comment
parser.add_argument('--name', '-n', dest='name', default=_type, help='Name of the collector instance.')
args = parser.parse_args()

start_hwm, end_hwm = get_hwm(args.name)

ext = Extractor(**load_config(args.config))
ext.test_connection()
ext.run(**{"start_hwm": start_hwm, "end_hwm": end_hwm})
#2024-05-30 00:00:00
#"start_hwm":
publish_hwm(_type, end_hwm)