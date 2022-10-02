import mqtt_logger
import os
import time

# Initalise mqtt recorder object
rec = mqtt_logger.Recorder(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    topics=["CROWSNEST/#"],
    broker_address="localhost",
    verbose=True,
    # username="admin",
    # password="verysecretadminpassword!"
)

# Start the logger, wait 10 seconds and stop the logger
rec.start()
# time.sleep(20)
input("Press Enter to STOP and Save")
rec.stop()