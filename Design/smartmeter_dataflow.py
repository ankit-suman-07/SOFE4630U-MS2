import argparse
import json
import logging


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

MEASUREMENTS = ("temperature", "humidity", "pressure")



def to_dict(message):
    # Pub/Sub delivers bytes; the producer sent a JSON dictionary
    return json.loads(message.decode("utf-8"))


def is_complete(record):
    # Drop records with a missing measurement (None, or an empty CSV cell sent as "")
    return all(record.get(k) not in (None, "") for k in MEASUREMENTS)


def convert(record):
    out = dict(record)
    out["temperature"] = float(record["temperature"]) * 1.8 + 32   # Celsius -> Fahrenheit
    out["pressure"] = float(record["pressure"]) / 6.895           # kPa -> psi
    out["humidity"] = float(record["humidity"])
    if record.get("time") not in (None, ""):
        out["time"] = float(record["time"])
    return out



def to_bytes(record):
    return json.dumps(record).encode("utf-8")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input topic: projects/<project>/topics/<topic>")
    parser.add_argument("--output", required=True, help="Output topic: projects/<project>/topics/<topic>")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (p
         | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input)
         | "To dict" >> beam.Map(to_dict)
         | "Filter" >> beam.Filter(is_complete)
         | "Convert" >> beam.Map(convert)
         | "To bytes" >> beam.Map(to_bytes)
         | "Write to PubSub" >> beam.io.WriteToPubSub(topic=known_args.output))



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()





