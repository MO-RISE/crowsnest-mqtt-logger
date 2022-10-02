# Crowsnest MQTT Logger & Playback

## Descriptions

MQTT topic logger with playback option. The local MQTT broker stream can be saved with: timestamp, massage and topic to an local SQLite database.

## Get started:

1. Set up [Crowsnest](https://github.com/MO-RISE/crowsnest) instance
2. Set up bridges to other MQTT streams for example Crowsnest broker according to: [Crowsnest MQTT Bridge](https://github.com/MO-RISE/crowsnest-bridge-mqtt/tree/8b980ddd6c224694dd0b3cce66a9e2a5bff23f8c)
3. Configure docker-compose environment variables:

   - RUN:Function definition of container ( LOGGER or PLAYBACK )
   - DB_NAME: SQLite DB name (default: "MQTT-LOG")
   - MQTT_TOPICS: Topics string separated by coma (CROWSNEST/#,TEST/#)
   - MQTT_PLAYBACK_DB: DB file name to be played back (Ex. 2022-10-02_MQTT-LOG.db)
   - MQTT_PLAYBACK_SPEED: Playback speed multiplayer (Default speed: 1 )

4. Run docker compose to start logging or playback 


Build from docker

```bash

docker-compose -f .\docker-compose.logger.yml up  

# Rebuild container 
docker-compose -f .\docker-compose.logger.yml up --build

```

[Based on Black-Hydon MQTT Logger](https://github.com/Blake-Haydon/mqtt-logger)

## License

Apache 2.0, see [LICENSE](./LICENSE)
