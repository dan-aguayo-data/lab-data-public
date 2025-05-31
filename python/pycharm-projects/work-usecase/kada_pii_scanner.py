import csv
import argparse
from kada_collectors.extractors.utils import load_config, get_generic_logger
from kada_collectors.extractors.pii_scanner import PIIScanner, VALID_DEFAULT_DETECTORS

get_generic_logger('root') # Set to use the root logger, you can change the context accordingly or define your own logger

parser = argparse.ArgumentParser(description='KADA PII Scanner.')
parser.add_argument('--extractor-config', '-e', dest='extractor_config', type=str, required=True, help='Location of the extarctor configuration json.')
parser.add_argument('--objects-file-path', '-f', dest='objects_file_path', type=str, required=True, help='Location of the .txt file that contains the list of objects to scan for the source.')
parser.add_argument('--source-type', '-t', dest='source_type', type=str, required=True, help='What kind of source are we scanning? E.g. snowflake, oracle etc. See documentation for full list of supported source types.')
parser.add_argument('--sample-size', '-s', dest='sample_size', type=int, required=True, help='How many rows are we sampling for each object, note that 0 means all rows will be sampled, number should be greater or equal to 0.')
parser.add_argument('--parrallel', '-p', dest='concurrency', type=int, default=1, help='Should it be running in parallel, default is 1 meaning no parallelism, consider your CPU resources when settings this value.')
parser.add_argument('--default-detectors', '-d', dest='default_detectors', type=str, help='Comma seperated list of default detectors the scanner should use {}'.format(','.join(VALID_DEFAULT_DETECTORS)))
parser.add_argument('--delta', '-a', dest='delta', action='store_true', help='Produces a DELTA extract file if you are doing a partial scan.')
parser.add_argument('--pii-output-path', '-o', dest='pii_output_path', type=str, required=True, help='The output path to the folder where the extract should appear.')
args = parser.parse_args()

# ######
# Impliment additional logic for checking existence if you wish
# You may also choose to call the PIIScanner differently and not use an input file this is completely up to you
# You can also feed in from your own custom producer for the list of objects
# This is simply the default out of the box implimentation to call the PIIScanner and produce the required Extract File for K
# ######

def read_validate_object_file(file_path):
    """
    Reads the flat file and validates the header and returns an iterator
    This is simply the out of the box way to feed the scanner, you may choose a different
    way to feed the scanner
    """
    with open (file_path, 'r', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        header = next(reader) # Skip the header
        if [x.upper() for x in header] != ['OBJECT_TYPE','OBJECT_ID']: # Should be a flat file thats comma delimited with the headers OBJECT_TYPE and OBJECT_ID
            raise Exception('Invalid object file')
        return [x for x in reader] # Return a list not an iterator as we will close the file

if __name__ == '__main__': # Do not omit this syntax as the Class impliments multiprocessing
    extractor_config = load_config(args.extractor_config) # Load the corresponding collector config file
    object_list = read_validate_object_file(args.objects_file_path) # 2D Array of objects
    # You can define your own Detector classes and register them before calling .scan() method to ensure the scanner picks up the new Detector Class, read the documentation on how to impliment new classes
    # You'll need to decorate the class with kada_collectors.extractors.pii_scanner.register_detector
    default_detectors = [x.strip() for x in args.default_detectors.split(',')] if args.default_detectors else []
    pii_scanner = PIIScanner(args.source_type, args.sample_size, args.concurrency, object_list, args.pii_output_path, default_detectors=default_detectors, delta=args.delta, **extractor_config)
    pii_scanner.scan()