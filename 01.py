#!/usr/bin/env python
"""
Read the 2013-09 "green taxi" CSV file from the New York City Taxi and
Limousine Commission (TLC) trip dataset, clean the data and write it to
a Parquet file.
"""

# stdlib imports
import re
from io import BytesIO
from pathlib import Path

# third-party imports
import pyarrow as pa
from pyarrow.csv import ConvertOptions, ParseOptions, ReadOptions, read_csv
from pyarrow.parquet import write_table
from smart_open import open as sopen


# CSV file encoding.
ENCODING = 'utf-8'


# It's expected that the first line of the CSV file will be header with
# the following column names in the same order.
HEADER = [
    'VendorID',
    'lpep_pickup_datetime',
    'Lpep_dropoff_datetime',
    'Store_and_fwd_flag',
    'RateCodeID',
    'Pickup_longitude',
    'Pickup_latitude',
    'Dropoff_longitude',
    'Dropoff_latitude',
    'Passenger_count',
    'Trip_distance',
    'Fare_amount',
    'Extra',
    'MTA_tax',
    'Tip_amount',
    'Tolls_amount',
    'Ehail_fee',
    'Total_amount',
    'Payment_type',
    'Trip_type',
]


# PyArrow data types that map to the following Parquet logical and
# physical types:
#
#   INTEGER → INT(bitWidth=16, isSigned=true), int32
#   TIMESTAMP → TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS), int64
#   BOOLEAN → boolean
#   DECIMAL_LON → DECIMAL(precision=18, scale=15), fixed_len_byte_array(8)
#   DECIMAL_LAT → DECIMAL(precision=17, scale=15), fixed_len_byte_array(8)
#   DECIMAL_DISTANCE → DECIMAL(precision=4, scale=2), fixed_len_byte_array(2)
#   DECIMAL_DOLLAR → DECIMAL(precision=6, scale=2), fixed_len_byte_array(3)
#
# INTEGER: Despite there being multiple integer-valued columns with
# different semantics (count and IDs), for simplicity I choose a 16-bit
# signed integer for all of them. All integer values in the data are
# small (passenger count is small and there are a small number of
# small-valued distinct IDs), but the max value of RateCodeID (99)
# is close enough to 128 that an 8-bit signed integer would make me
# uncomfortable. Also, despite there being no negative integer values, I
# opted for a signed integer type because some databases (e.g. Vertica,
# an OLAP database) only support signed integers.
#
# TIMESTAMP: The source data has timestamps with second precision. The
# data dictionary
#
#   https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
#
# does not indicate the time zone of the timestamps, so I assume they
# are in local time ("America/New_York" time zone). To be more sure, I
# could investigate the times of day at which taxis are most and least
# frequent assuming Ameria/New_York or UTC and see which makes more
# sense. See also:
#
#   https://www.kaggle.com/c/new-york-city-taxi-fare-prediction/discussion/64203
#
# The timestamps will be converted to UTC and stored as such in the
# Parquet file, but since the stored timestamps are timezone-aware, they
# may easily be converted back to America/New_York if desired.
#
# DECIMAL_{LON,LAT}: Precisions of 18 and 17 are almost certainly
# unnecessarily high for real-world applications, but to preserve all 15
# fractional digits along with a possible max of three integral digits
# for longitude and two integral digits for latitude, precisions of 18
# and 17 have been respectively chosen. If instead double precision
# numbers were used, rounding errors would occur. For example, the
# number
#
#   -73.952407836914062
#
# is in the source data but cannot be represented exactly as binary
# floating point number of any precision because it's equal to
#
#   -36976203918457031 / 500000000000000
#
# and the denominator is not a power of two.
#
# DECIMAL_DISTANCE: It seems unlikely that a taxi would travel 100 miles
# or more within NYC, and indeed this does not occur within the source
# data, so a precision of 4 is sufficient to preserve the two fractional
# digits from the source data.
#
# DECIMAL_DOLLAR: It seems unlikely that a total dollar amount would
# equal $10,000 or more, so a precision of 6 is sufficient to preserve
# the two fractional digits from the source data.
INTEGER = pa.int16()
TIMESTAMP = pa.timestamp(unit='s', tz='America/New_York')
BOOLEAN = pa.bool_()
DECIMAL_LON = pa.decimal128(precision=18, scale=15)
DECIMAL_LAT = pa.decimal128(precision=17, scale=15)
DECIMAL_DISTANCE = pa.decimal128(precision=4, scale=2)
DECIMAL_DOLLAR = pa.decimal128(precision=6, scale=2)


# The CSV file will be parsed as a PyArrow table with the following
# schema.
#
# Note: the inconsistent capitalization of "lpep_pickup_datetime" and
# "Lpep_dropoff_datetime" is adjusted here to match the data dictionary:
#
#   https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
SCHEMA = pa.schema([
    ('VendorID', INTEGER),
    ('lpep_pickup_datetime', TIMESTAMP),
    ('lpep_dropoff_datetime', TIMESTAMP),
    ('Store_and_fwd_flag', BOOLEAN),
    ('RateCodeID', INTEGER),
    ('Pickup_longitude', DECIMAL_LON),
    ('Pickup_latitude', DECIMAL_LAT),
    ('Dropoff_longitude', DECIMAL_LON),
    ('Dropoff_latitude', DECIMAL_LAT),
    ('Passenger_count', INTEGER),
    ('Trip_distance', DECIMAL_DISTANCE),
    ('Fare_amount', DECIMAL_DOLLAR),
    ('Extra', DECIMAL_DOLLAR),
    ('MTA_tax', DECIMAL_DOLLAR),
    ('Tip_amount', DECIMAL_DOLLAR),
    ('Tolls_amount', DECIMAL_DOLLAR),
    ('Ehail_fee', DECIMAL_DOLLAR),
    ('Total_amount', DECIMAL_DOLLAR),
    ('Payment_type', INTEGER),
    ('Trip_type', INTEGER),
])


# Data records may have additional fields not specified in the header.
# These extra fields appear at the end of a record, so define a regex
# that may extract only the desired leftmost fields.
PATTERN_DATA = re.compile(rb'\A((?:[^,]*,){%d}[^,\r\n]*)' % (len(HEADER) - 1))


class InvalidHeaderError(Exception):
    pass


class InvalidDataError(Exception):
    pass


def read_green_taxi_csv(url, fobj):
    """
    Read a "green taxi" CSV file from the New York City Taxi and
    Limousine Commission (TLC) trip dataset, clean the data and write
    the data into the provided binary file object.

    Note: tested against only one specific file:

      https://nyc-tlc.s3.us-east-1.amazonaws.com/trip%20data/green_tripdata_2013-09.csv
    """

    # smart-open makes it easy to open a file via HTTP(S), S3, GCS,
    # local etc URLs.
    with sopen(url, mode='rb') as fobj_src:
        # The first line should be the header. Validate that it's what
        # we expect.
        line = fobj_src.readline()
        if line.rstrip().decode(ENCODING).split(',') != HEADER:
            raise InvalidHeaderError(line)
        # Ignore any whitespace-only lines between the header and data.
        # Return if we encounter the end of the file.
        while True:
            offset = fobj_src.tell()
            line = fobj_src.readline()
            if line.rstrip():
                fobj_src.seek(offset)
                break
            if not line:
                return
        # Ensure that there are at least 20 fields and preserve only
        # these fields via regexp. The data has an odd structure in that
        # there are additional trailing empty fields, which we ignore.
        for line in fobj_src:
            match = PATTERN_DATA.match(line)
            if not match:
                raise InvalidDataError(line)
            fobj.write(match.group(1))
            fobj.write(b'\n')


def parse_green_taxi_csv(fobj):
    """
    Parse a binary file object of cleaned "green taxi" CSV data as
    returned by the "read_green_taxi_csv" function, and return a PyArrow
    table.
    """

    convert_options = ConvertOptions(
        column_types=SCHEMA,
        false_values=['N'],
        null_values=[''],
        timestamp_parsers=['%Y-%m-%d %H:%M:%S'],
        true_values=['Y'],
    )
    parse_options = ParseOptions(quote_char=False)
    read_options = ReadOptions(
        column_names=SCHEMA.names,
        encoding=ENCODING,
    )

    return read_csv(
        fobj,
        convert_options=convert_options,
        parse_options=parse_options,
        read_options=read_options,
    )


def write_table_to_parquet(table, path):
    """
    Write a PyArrow table to a Parquet file.
    """
    write_table(table, path, version='2.0')


def main():
    # Typically I would use argparse to allow these to be supplied by
    # the user, although in this case since I have tested with only one
    # source file, I've hardcoded these for simplicity.
    url = (
        'https://nyc-tlc.s3.us-east-1.amazonaws.com/trip%20data/'
        'green_tripdata_2013-09.csv'
    )
    path = Path('01.parquet')

    # Read/clean the source data into a binary buffer, then parse that
    # buffer into a PyArrow table.
    with BytesIO() as buf:
        read_green_taxi_csv(url, buf)
        buf.seek(0)
        table = parse_green_taxi_csv(buf)

    # Write the PyArrow table to a Parquet file.
    write_table_to_parquet(table, path)


if __name__ == '__main__':
    main()
