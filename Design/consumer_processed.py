from google.cloud import pubsub_v1
import glob, json, os


script_dir = os.path.dirname(os.path.abspath(__file__))
files = glob.glob(os.path.join(script_dir, "*.json"))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "project-3bf0b424-c8ee-4589-bd7"
subscription_id = "designLabelsProcessed-sub"   # output of the Dataflow job


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    record = json.loads(message.data.decode("utf-8"))   # deserialize
    print(f"{record.get('profileName')}: "
          f"temperature {record['temperature']:.2f} F, "
          f"humidity {record['humidity']:.2f} %, "
          f"pressure {record['pressure']:.4f} psi")
    message.ack()

with subscriber:
    future = subscriber.subscribe(subscription_path, callback=callback)
    print("Listening on", subscription_path)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()






