"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)

LIST_STATUS = [
    "sunny", 
    "partly_cloudy", 
    "cloudy", 
    "windy", 
    "precipitation",
]


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info("weather process_message is incomplete - skipping")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        value = json.loads(message.value())
        self.temperature = value.get("temperature")
        status_idx = value.get("status")
        if status_idx:
            self.status = LIST_STATUS[status_idx]
        else:
            self.status = None

