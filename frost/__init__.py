import sys
import os
import logging
from typing import Optional

import pendulum
from pendulum.datetime import DateTime
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

logger = logging.getLogger("frost")

try:
    import requests_cache

    requests_cache.install_cache("frost")
except:
    pass

BASE_URL = "https://frost.met.no"

PROMSCALE_WRITE_URL = "https://promscale.service.ruud.cloud/write"
PROMSCALE_QUERY_URL = "https://promscale.service.ruud.cloud/api/v1/query"
PROMSCALE_CERT_PATH = None

#DEFAULT_FROST_SENSOR_ID = "SN28380" # Kongsberg
DEFAULT_FROST_SENSOR_ID = "SN19780" # Vollen


def get_available_timeseries(session: requests.Session):
    url = BASE_URL + "/observations/availableTimeSeries/v0.jsonld"
    return session.get(url, params={"sources": FROST_SENSOR_ID}).json()


def get_observation_samples(
    session: requests.Session,
    element_id: str,
    metric_name: str,
    from_time: Optional[DateTime] = None,
    to_time: Optional[DateTime] = None,
):
    params = {
        "sources": FROST_SENSOR_ID,
        "referencetime": f"{from_time.to_iso8601_string()}/{to_time.to_iso8601_string()}"
        if from_time and to_time
        else "latest",
        "elements": element_id,
    }
    url = BASE_URL + "/observations/v0.jsonld"
    data = session.get(url, params=params).json()

    if "error" in data:
        raise ValueError(f"{data['error']['message']} - {data['error']['reason']}")

    measurements = data["data"]
    samples = []
    for measurement in measurements:
        sample = []

        sample.append(1000 * pendulum.parse(measurement["referenceTime"]).int_timestamp)

        observation = measurement["observations"][0]
        sample.append(float(observation["value"]))

        samples.append(sample)

    timeseries = {
        "labels": {
            "__name__": metric_name,
            "location": "outside",
            "sensor": FROST_SENSOR_ID,
        },
        "samples": samples,
    }

    return timeseries



def get_last_timestamp_in_metric(metric_name: str, lookback: str = "1d"):
    res = requests.get(
        PROMSCALE_QUERY_URL,
        params={"query": f"max_over_time(timestamp({metric_name})[{lookback}:])"},
        verify=PROMSCALE_CERT_PATH if PROMSCALE_CERT_PATH else False,
    )
    if not 200 <= res.status_code < 300:
        raise ValueError(f"Unable to get last timestamp of '{metric_name}': {res.text}")

    query_response = res.json()
    query_result = query_response["data"]["result"]
    logger.debug(f"Query result from getting last timestamp: {query_result}")

    try:
        timestamp_s = query_result[0]["value"][1]
    except IndexError as e:
        if lookback != "30d":
            return get_last_timestamp_in_metric(metric_name, "30d")

        logger.warning("Query to get last metric timestamp returned nothing, using start of month..")
        return pendulum.now().start_of('month')

    last_metric_timestamp = pendulum.from_timestamp(float(timestamp_s))

    return last_metric_timestamp


def main():
    logging.basicConfig(level="INFO")
    load_dotenv()

    global PROMSCALE_CERT_PATH
    PROMSCALE_CERT_PATH = os.getenv("PROMSCALE_CERT_PATH")

    global FROST_SENSOR_ID
    FROST_SENSOR_ID = os.getenv("FROST_SENSOR_ID") or DEFAULT_FROST_SENSOR_ID

    if PROMSCALE_CERT_PATH:
        logger.info(f"Using '{PROMSCALE_CERT_PATH}' as promscale certificate")
    else:
        logger.info("Will not verify certificate")

    if len(sys.argv) >= 3:
        start_time = pendulum.parse(sys.argv[1])
        end_time = pendulum.parse(sys.argv[2])
    else:
        logger.info("Did not get any timestamps, running from last metric (if exists) to now")

        start_time = None
        end_time = pendulum.now(tz="UTC")

    session = requests.session()
    session.auth = HTTPBasicAuth(os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET"))

    error_count = 0
    for element_id, metric_name in [
        ("air_temperature", "temperature_met"),
        ("relative_humidity", "humidity_met"),
    ]:
        try:
            start_time_ = (
                start_time
                if start_time
                else get_last_timestamp_in_metric(metric_name) + pendulum.duration(0, 1)
            )
            if end_time < start_time_:
                logger.warning(
                    "Have a future (?) value in promscale, something is off.."
                )
                continue

            logger.info(
                f"Fetching '{element_id}' from '{BASE_URL}' from '{start_time_}' to '{end_time}'"
            )
            timeseries = get_observation_samples(
                session, element_id, metric_name, start_time_, end_time
            )
            logger.info(f"Got {len(timeseries['samples'])} '{element_id}' samples")

            logger.info(f"Sending samples to promscale")
            res = requests.post(
                PROMSCALE_WRITE_URL,
                json=timeseries,
                verify=PROMSCALE_CERT_PATH if PROMSCALE_CERT_PATH else False,
            )
            if 200 <= res.status_code < 300:
                logger.info(f"Successfully ingested '{element_id}' samples")
            else:
                logger.error(
                    f"Unable to ingest '{element_id}' samples, got '{res.status_code}'"
                )
        except Exception as e:
            if 'No data found' in str(e):
                logger.info(f"No new data for '{element_id}', continuing...")
                continue

            logger.error(f"Unable to fetch data for '{metric_name}': {e}")
            error_count = error_count + 1


    if error_count > 0:
        logger.error(f"Fetching data for '{error_count}' metrics failed")
        exit(1)


if __name__ == "__main__":
    main()
