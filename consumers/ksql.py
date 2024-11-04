"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"
TURNSTILE_TOPIC_NAME = "org.chicago.cta.stations.turnstile"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

# My idea using stream instead to keep all turnstile having same station_id, but different line
# The summary table is created from stream
# Apply window session to add turnstile having same key into one window
KSQL_STATEMENT = f"""
CREATE STREAM turnstile_stream (
    station_id varchar,
    station_name varchar,
    line varchar
)
WITH (
    KAFKA_TOPIC='{TURNSTILE_TOPIC_NAME}',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);
    

CREATE TABLE turnstile_summary WITH (
    VALUE_FORMAT='JSON',
    KAFKA_TOPIC='TURNSTILE_SUMMARY'
) AS
SELECT
    station_id AS STATION_ID,
    count(*) AS COUNT
FROM turnstile_stream
WINDOW SESSION (20 MINUTES)
GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps({
            "ksql": KSQL_STATEMENT,
            "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
        }),
    )

    # Ensure that a 2XX status code was returned
    try:
        ## Ensure a healthy response was given
        resp.raise_for_status()
        logging.debug("KSQL created table successfully")
    except requests.HTTPError as e:
        logger.error(f"Error run KSQL TURNSTILE_SUMMARY: {str(e)}")
        raise


if __name__ == "__main__":
    execute_statement()
