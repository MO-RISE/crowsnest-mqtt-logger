from environs import Env
import mqtt_logger
import os
import time
from datetime import datetime


# Reading config from environment variables
env = Env()
RUN = env("RUN", "LOGGER")
DB_NAME = env("DB_NAME", "MQTT-LOG")
MQTT_TOPICS = env("MQTT_TOPICS", "#")
MQTT_PLAYBACK_DB = env("MQTT_PLAYBACK_DB")
MQTT_PLAYBACK_SPEED = env("MQTT_PLAYBACK_SPEED", 1)


def start_recording():

    # Topics to list
    print("Topics", MQTT_TOPICS)
    mqtt_topics = MQTT_TOPICS.split(",")

    # DB path and name
    current_date = datetime.now()
    db_path_and_name = f"DB/{current_date.date()}_{DB_NAME}.db"

    # Initalise mqtt recorder object
    rec = mqtt_logger.Recorder(
        sqlite_database_path=os.path.join(os.path.dirname(__file__), db_path_and_name),
        topics=mqtt_topics,
        broker_address="localhost",
        verbose=True,
        # username="user",
        # password="password"
    )

    # Start the logger, wait 10 seconds and stop the logger
    rec.start()
    # time.sleep(20)
    # input("Press Enter to STOP and Save")
    # rec.stop()
    print("Logging...")

    while True:
        try:
            time.sleep(1)
        except:
            print("Stopping logging")
            rec.stop()
            print("Stopped")


def start_playback():
    # Initalise playback object
    playback = mqtt_logger.Playback(
        sqlite_database_path=os.path.join(os.path.dirname(__file__), f"DB/{MQTT_PLAYBACK_DB}"),
        broker_address="localhost",
        verbose=True,
    )

    # Start playback at 2x speed (twice as fast)
    playback.play(speed=1)
    


def main():

    if RUN == "PLAYBACK":
        print("Starting REPLAY")
        start_playback()
    else:
        print("Starting LOGGING")
        start_recording()


if __name__ == "__main__":
    main()
