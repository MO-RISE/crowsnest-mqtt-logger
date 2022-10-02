import mqtt_logger
import os

# Initalise playback object
playback = mqtt_logger.Playback(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    broker_address="localhost",
    verbose=True,
)

# Start playback at 2x speed (twice as fast)
playback.play(speed=1)