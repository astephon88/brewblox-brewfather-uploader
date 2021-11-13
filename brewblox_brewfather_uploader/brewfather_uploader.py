"""
Example on how to set up a feature that polls data, and publishes to the eventbus.
"""

import asyncio

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

    def derive_metric(self, brewfather_field, sensor_config):

        gravity_unit_string = 'Specific gravity' if self.gravity_unit == 'G' else 'Plato[degP]'

        temp_metrics = {
                        'tilt': {
                            True: f'Calibrated temperature[deg{self.temp_unit}]',
                            False: f'Temperature[deg{self.temp_unit}]'
                         },
                        'spark': {
                            True: f'value[deg{self.temp_unit}]'
                        }
        }

        metric_matrix = {
            'temp': temp_metrics,
            'aux_temp': temp_metrics,
            'ext_temp': temp_metrics,
            'gravity': {
                'tilt': {
                    True: f'Calibrated {gravity_unit_string[0].lower()+gravity_unit_string[1:]}',
                    False: f'{gravity_unit_string}'
                }
            }
        }
        metric_suffix_str = metric_matrix \
            .get(brewfather_field, {}) \
            .get(sensor_config['service_type'], {}) \
            .get(sensor_config.get('calibrated', True), None)

        return f'{sensor_config["service"]}/{sensor_config["sensor"]}/' \
            + metric_suffix_str if metric_suffix_str else None

    async def prepare(self):
        """
        This function must be implemented by child classes of RepeaterFeature.
        It is called once after the service started.
        """
        LOGGER.info(f'Starting {self}')

        # Get values from config
        history_host = self.app['config']['history_host']
        history_port = self.app['config']['history_port']
        self.metrics_url = f'{history_host}:{history_port}/history/timeseries/metrics'

        self.name = self.app['config']['name']
        self.interval = max(900.0, self.app['config']['poll_interval'])

        config_filename = self.app['config']['metrics_config_file']
        try:
            # load the config file
            with open(config_filename) as yamlfile:
                config_file = safe_load(yamlfile)

            # extract the global settings
            self.brewfather_url = config_file['settings']['brewfather_url']
            # todo: get this from the datastore
            self.temp_unit = config_file['settings'].get('temp_unit', 'C')
            # todo: get this from the datastore once implemented
            self.gravity_unit = config_file['settings'].get('gravity_unit', 'G')

            self.field_mapping = dict()

            # loop through each fermenter in the config file
            for fermenter in config_file['fermentations']:
                # extract the fermenter name and fields from the config file

                self.field_mapping[fermenter['name']] = {
                    brewfather_field: self.derive_metric(brewfather_field, sensor_config)
                    for brewfather_field, sensor_config in fermenter['sensors'].items()
                }

        except Exception as e:
            LOGGER.error('Error loading fermenter configuration file: %s', config_filename, exc_info=True)
            raise repeater.RepeaterCancelled from e

    async def run(self):
        """
        This function must be implemented by child classes of RepeaterFeature.
        After prepare(), the base class keeps calling run() until the service shuts down.
        """

        # These are available because we called the setup functions in __main__
        # If you ever get a KeyError when trying to get these, you forgot to call setup()
        session = http.session(self.app)

        for fermenter_name, fields in self.field_mapping.items():
            brewblox_params = {
                'fields': list
                (
                    {
                        metric
                        for metric in fields.values()
                        if metric is not None
                    }
                )
            }
            LOGGER.debug('Submitted brewblox fields: %s', brewblox_params)
            try:
                response = await session.post(self.metrics_url, json=brewblox_params)
                response_values = await response.json()
                LOGGER.debug('Returned brewblox metrics: %s', response_values)
                bfdata = {
                    brewfather_field: response_value['value']
                    for brewfather_field in ['temp', 'gravity', 'aux_temp', 'ext_temp']
                    for response_value in response_values
                    if fields.get(brewfather_field, None) == response_value['metric']
                }

            except ClientResponseError:
                LOGGER.error(
                    'Request to Brewblox API failed',
                    exc_info=True
                )

            # add name and temp units
            bfdata['name'] = fermenter_name
            bfdata['temp_unit'] = self.temp_unit
            bfdata['gravity_unit'] = self.gravity_unit

            # clear out any empty fields. todo: This probably isn't actually necessary
            brewfather_params = {
                k: v
                for k, v in bfdata.items()
                if v is not None
            }

            LOGGER.debug('Submitted brewfather fields: %s', brewfather_params)
            try:
                bf_response = await session.post(self.brewfather_url, json=brewfather_params)

                # have to disable mime-type checking because brewfather uses text/html, even with an Accept header
                result = (await bf_response.json(content_type=None))['result']
                if result == 'success':
                    LOGGER.info('Data submitted successfully')
                # for some reason, the result is 'OK' now instead of 'ignored' beacause...reasons?
                elif result == 'OK' or result == 'ignored':
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
