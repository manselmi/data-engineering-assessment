"""
Read the Parquet file generated in part 1, add some derived columns and
write the result to another Parquet file.
"""

# stdlib imports
import shutil
import textwrap
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

# third-party imports
from pyspark.sql import SparkSession


def build_query(table_alias):
    """
    Build a Spark SQL query satisfying the requirements of the exercise.
    """

    template = textwrap.dedent("""\
        SELECT
        *,
        {one_hot_hour},
        {one_hot_dow},
        {duration},
        {jfk}
        FROM
        {table_alias}""")

    # Generate SQL to select a one-hot encoding of the pickup hour of
    # the day. The instructions don't specify which timestamp(s) we care
    # about here, so I've chosen the pickup timestamp. The containerized
    # Spark environment runs in a UTC timezone and the timestamps are in
    # "local" (UTC) time; for simplicity we'll keep that as-is and not
    # convert to local time.
    one_hot_hour = StringIO()
    num_hours = 24
    for hour in range(num_hours):
        one_hot_hour.write(
            f'CASE WHEN HOUR(lpep_pickup_datetime) = {hour} THEN 1 '
            f'ELSE 0 END AS Pickup_hour_is_{hour}'
        )
        if hour < num_hours - 1:
            one_hot_hour.write(',\n')

    # Generate SQL to select a one-hot encoding of the pickup day of
    # week. The instructions don't specify which timestamp(s) we care
    # about here, so I've chosen the pickup timestamp. The containerized
    # Spark environment runs in a UTC timezone and the timestamps are in
    # "local" (UTC) time; for simplicity we'll keep that as-is and not
    # convert to local time.
    #
    # Here, day 0 corresponds to Sunday and day 6 corresponds to
    # Saturday.
    one_hot_dow = StringIO()
    num_days = 7
    for dow in range(num_days):
        one_hot_dow.write(
            f'CASE WHEN DAYOFWEEK(lpep_pickup_datetime) = {dow} THEN 1 '
            f'ELSE 0 END AS Pickup_dow_is_{dow}'
        )
        if dow < num_days - 1:
            one_hot_dow.write(',\n')

    # SQL to select the ride duration in seconds. Duration in seconds
    # may be computed by casting to the number of seconds elapsed since
    # the POSIX epoch and subtracting.
    #
    # Interestingly, some negative durations are computed, which
    # suggests an issue with the source data.
    duration = (
        'CAST(lpep_dropoff_datetime AS LONG) - '
        'CAST(lpep_pickup_datetime AS LONG) AS Duration_seconds'
    )

    # SQL to determine whether a pickup or dropoff occurred at JFK
    # airport. Determining a bounding box is nontrivial and I figure
    # it's outside of the scope of this exercise, so I instead looked up
    # a bounding box online. Amusingly, I found a page written by
    # someone else working with this dataset:
    #
    #   https://chriswhong.com/open-data/should-i-stay-or-should-i-go-nyc-taxis-at-the-airport/
    lon_min = -73.794694
    lon_max = -73.776283
    lat_min = 40.640668
    lat_max = 40.651381
    jfk = (
        f'CASE WHEN (Pickup_longitude BETWEEN {lon_min} AND {lon_max} '
        f'AND Pickup_latitude BETWEEN {lat_min} AND {lat_max}) OR '
        f'(Dropoff_longitude BETWEEN {lon_min} AND {lon_max} AND '
        f'Dropoff_latitude BETWEEN {lat_min} AND {lat_max}) THEN 1 '
        f'ELSE 0 END AS Pickup_or_dropoff_at_JFK'
    )

    return template.format(
        one_hot_hour=one_hot_hour.getvalue(),
        one_hot_dow=one_hot_dow.getvalue(),
        duration=duration,
        jfk=jfk,
        table_alias=table_alias,
    )


def main():
    # Typically I would use argparse to allow these to be supplied
    # by the user, although in this case I've hardcoded these for
    # simplicity.
    input_url = 'file:///mnt/workspace/01.parquet'
    output_file = Path('/mnt/workspace/02.parquet')

    # To more easily provide a single Parquet file for inspection (and
    # not within the directory automatically created by Hadoop/Spark),
    # we have Spark write its results to a temporary directory and then
    # later move the single Parquet file outside of that directory.
    with TemporaryDirectory() as output_dir:
        output_dir = Path(output_dir)
        output_url = f'file://{output_dir}'

        with SparkSession.builder.getOrCreate() as spark:
            # Read the file from the part 1.
            input_df = spark.read.parquet(input_url)

            # Build the SQL query and execute it.
            input_sql_alias = 'input_df'
            input_df.createOrReplaceTempView(input_sql_alias)
            output_df = spark.sql(build_query(input_sql_alias))

            # Write the result in Parquet format.
            output_df.coalesce(1).write.parquet(
                output_url,
                mode='overwrite',
            )

        # Move the Parquet file so that it's accessible outside of the
        # container.
        shutil.move(next(output_dir.glob('*.parquet')), output_file)


if __name__ == '__main__':
    main()
