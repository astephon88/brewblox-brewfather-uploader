"""
Example on how to set up a feature that polls data, and publishes to the eventbus.
"""

import asyncio
from typing import Any, Dict, List

from aiohttp import web, ClientResponseError
from brewblox_service import brewblox_logger, features, http, repeater
from yaml import safe_load
LOGGER = brewblox_logger(__name__)


class PublishingFeature(repeater.RepeaterFeature):
    """
    repeater.RepeaterFeature is a base class for a common use case:
    - prepare
    - every X seconds, do Y, until the service shuts down
    """

    async def prepare(self):
        """
        This function must be implemented by child classes of RepeaterFeature.
        It is called once after the service started.
        """
        LOGGER.info(f'Starting {self}')

        # Get values from config
        config_file = self.app['config']['fermenter_config_file']
        history_host = self.app['config']['history_host']
        history_port = self.app['config']['history_port']
        self.name = self.app['config']['name']
        self.interval = max(900.0, self.app['config']['poll_interval'])

        self.metrics_url = f'{history_host}:{history_port}/history/timeseries/metrics'
        self.brewfather_url = self.app['config']['brewfather_url']

        # get fermenter configuration from config file and map brewblox metrics to brewfather fields.
        # this is ugly, and there's got to be a better way
        try:
            yamlfile = open(config_file)
            fermenter_config: List[Dict[str, Any]] = safe_load(yamlfile)

            self.field_mapping: Dict[str, Dict[str, Dict[str, str]]] = {}

            # loop through each fermenter in the config file
            for fermenter in fermenter_config:
                # extract the fermenter name and fields from the config file
                fermenter_name: str = fermenter['name']
                # extract the measurement units if they are available
                temp_unit: str = fermenter.get('temp_unit', 'F')
                gravity_unit: str = fermenter.get('gravity_unit', 'G')
                fermenter_sensors: Dict[str, Dict[str, Any]] = fermenter['sensors']

                # initialize the optinal brewfather fields to None so we can exclude them later if they aren't used
                temp_metric: str = None
                gravity_metric: str = None

                # loop through each sensor config
                for brewfather_field, sensor_config in fermenter_sensors.items():
                    # extract the common sensor config values
                    service_type: str = sensor_config['service_type']
                    service_name: str = sensor_config['service']
                    sensor: str = sensor_config['sensor']

                    # derive the brewlox temp metric based on the sensor configuration
                    if brewfather_field == 'temp':

                        if service_type == 'tilt':
                            if sensor_config.get('tilt_params', {}).get('calibrated', False):
                                temp_metric = f'{service_name}/{sensor}/Calibrated temperature[deg{temp_unit}]'
                            else:
                                temp_metric = f'{service_name}/{sensor}/Temperature[deg{temp_unit}]'
                        elif service_type == 'spark':
                            temp_metric = f'{service_name}/{sensor}/value[deg{temp_unit}]'

                        # todo: elif other temp sensor services

                    # derive the brewblox gravity metric based on the sensor configuration
                    elif brewfather_field == 'gravity':

                        if service_type == 'tilt':
                            unit_string = 'Specific gravity' if gravity_unit == 'G' else 'Plato[degP]'
                            if sensor_config.get('tilt_params', {}).get('calibrated', False):
                                gravity_metric = f'{service_name}/{sensor}/Calibrated {unit_string.lower()}'
                            else:
                                gravity_metric = f'{service_name}/{sensor}/{unit_string}'

                        # todo: elif other gravity sensor services. i.e. plaato
                    # todo: elif other brewfather fields. i.e. aux_temp, ext_temp, bpm, pressure

                # create a mapping from brewfather field to brewblox metric
                self.field_mapping[fermenter_name] = {
                    'metrics': {
                        'temp':  temp_metric,
                        'gravity': gravity_metric
                    },
                    'units': {
                        'temp_unit': temp_unit,
                        'gravity_unit': gravity_unit
                    }

                    # todo: add aux_temp, ext_temp, bpm, and pressure
                }

        except Exception as e:
            LOGGER.error('Error loading fermenter configuration file: %s', config_file, exc_info=True)
            raise repeater.RepeaterCancelled from e

        # You can prematurely exit here.
        # Raise RepeaterCancelled(), and the base class will stop without a fuss.
        # run() will not be called.
        if self.interval <= 0:
            raise repeater.RepeaterCancelled()

    async def run(self):
        """
        This function must be implemented by child classes of RepeaterFeature.
        After prepare(), the base class keeps calling run() until the service shuts down.
        """

        # These are available because we called the setup functions in __main__
        # If you ever get a KeyError when trying to get these, you forgot to call setup()
        session = http.session(self.app)

        for fermenter_name, fields in self.field_mapping.items():
            metrics = fields['metrics']
            brewblox_params = {
                'fields': list
                (
                    {
                        metric
                        for metric in metrics.values()
                        if metric is not None
                    }
                )
            }

            response = await session.post(self.metrics_url, json=brewblox_params)

            try:
                bfdata = {}
                for response_value in await response.json():
                    brewblox_field = response_value['metric']
                    if fields['metrics']['temp'] == brewblox_field:
                        bfdata['temp'] = response_value['value']
                    elif fields['metrics']['gravity'] == brewblox_field:
                        bfdata['gravity'] = response_value['value']

            except ClientResponseError:
                LOGGER.error(
                    'Request to Brewblox API failed',
                    exc_info=True
                )

            # add name and temp units
            bfdata['name'] = fermenter_name
            bfdata['temp_unit'] = fields['units']['temp_unit']
            bfdata['gravity_unit'] = fields['units']['gravity_unit']

            # clear out any empty fields
            brewfather_params = {
                k: v
                for k, v in bfdata.items()
                if v is not None
            }

            try:
                bf_response = await session.post(self.brewfather_url, json=brewfather_params)

                # have to disable mime-type checking because brewfather uses text/html, even with an Accept header
                result = (await bf_response.json(content_type=None))['result']
                if result == 'success':
                    LOGGER.info('Data submitted successfully')
                elif result == 'ignored':
                    LOGGER.warning('Data submission ignored. (Leave at least 900 seconds between logging)')
                else:
                    LOGGER.warning('%s', await bf_response.text())
            except ClientResponseError:
                LOGGER.error('Request to Brewfather API failed', exc_info=True)

        """
        To prevent spam, it is strongly recommended to use asyncio.sleep().
        asyncio.sleep() is non-blocking - other services and endpoint handlers will run.
        """
        await asyncio.sleep(self.interval)


def setup(app: web.Application):
    # We register our feature here
    # It will now be automatically started when the service starts
    features.add(app, PublishingFeature(app))


def fget(app: web.Application) -> PublishingFeature:
    # Retrieve the registered instance of PublishingFeature
    return features.get(app, PublishingFeature)
