import json
import boto3


global config_parameters
if 'config_parameters' not in globals():
    config_parameters = {}


class ConfigParameter(object):
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def get_name(self):
        return self.name

    def get_cli_name_flag(self):
        result = '--'
        for letter in self.name:
            if letter.isupper():
                result += '-'
            result += letter.lower()
        return result

    def get_value(self):
        if str(self.value).lower() == 'true':
            return True
        if str(self.value).lower() == 'false':
            return False
        return self.value


class ConfigParameterFactory:
    def __init__(self):
        pass

    @staticmethod
    def make_default_config_parameter(name, value, description, possible_values):
        if not isinstance(possible_values, list):
            possible_values = possible_values.split('|')
        possible_values = [a.lower() for a in possible_values]
        if ConfigParameterFactory.is_bool_list(possible_values):
            return ConfigParameterFactory.make_bool_config_parameter(name, value, description)
        elif ConfigParameterFactory.is_region_name_list(possible_values):
            return ConfigParameterFactory.make_region_config_parameter(name, value, description)
        else:
            return ConfigParameterFactory.make_value_list_config_parameter(name, value, description,
                                                                           possible_values)

    @staticmethod
    def try_make_new_default_config_parameter(default_config_parameter, new_value):
        return ConfigParameterFactory.make_default_config_parameter(default_config_parameter.name,
                                                                    new_value,
                                                                    default_config_parameter.description,
                                                                    default_config_parameter.possible_values)

    @staticmethod
    def make_config_parameter(default_config_parameter, new_value):
        config_parameter = ConfigParameterFactory.try_make_new_default_config_parameter(default_config_parameter,
                                                                                        new_value)
        return ConfigParameter(config_parameter.name, config_parameter.value)

    @staticmethod
    def make_bool_config_parameter(name, value, description):
        return DefaultBoolConfigParameter(name, value, description)

    @staticmethod
    def make_region_config_parameter(name, value, description):
        return DefaultRegionConfigParameter(name, value, description)

    @staticmethod
    def make_value_list_config_parameter(name, value, description, possible_values):
        return DefaultValueListConfigParameter(name, value, description, possible_values)

    @staticmethod
    def is_bool_list(values_list):
        if 'true' in values_list and 'false' in values_list and len(values_list) == 2:
            return True
        else:
            return False

    @staticmethod
    def is_region_name_list(values_list):
        if 'short-region-name' in values_list:
            return True
        else:
            return False


class DefaultConfigParameter(ConfigParameter):
    def __init__(self, name, value, description, possible_values):
        super(DefaultConfigParameter, self).__init__(name, value)
        self.description = description
        self.possible_values = possible_values
        self.value_checks = []
        self.type = 'generic'

    def is_possible_value_for_config_parameter(self, value):
        for value_check in self.value_checks:
            value_check(value)
        return True

    class InvalidConfigException(Exception):
        def __init__(self, reason):
            self.reason = reason

        def __str__(self):
            return self.reason


class DefaultValueListConfigParameter(DefaultConfigParameter):
    # noinspection PyDefaultArgument
    def __init__(self, name, value, description, possible_values=[]):
        super(DefaultValueListConfigParameter, self).__init__(name, value, description, possible_values)
        self.possible_values = [a.lower() for a in possible_values]
        self.type = 'value-list'
        self.value_checks = [self.check_is_value_in_list]

    def is_value_in_list(self, value):
        str_value = str(value)
        if str_value.lower() in self.possible_values:
            return True
        else:
            return False

    def check_is_value_in_list(self, value):
        if not self.is_value_in_list(value):
            error_message = 'Unexpected value {value} for parameter {parameter}, expected {expected}'.format(
                value=value,
                parameter=self.name,
                expected=self.possible_values
            )
            raise DefaultValueListConfigParameter.InvalidValueListInConfigException(error_message)

    class InvalidValueListInConfigException(DefaultConfigParameter.InvalidConfigException):
        def __init__(self, reason):
            self.reason = reason


class DefaultRegionConfigParameter(DefaultConfigParameter):
    # noinspection PyDefaultArgument
    def __init__(self, name, value, description, possible_values=[]):
        super(DefaultRegionConfigParameter, self).__init__(name, value, description, possible_values)
        self.value_checks = [self.check_is_region]
        self.type = 'region'
        self.valid_regions = None
        self.possible_values = 'short-region-name'
        if self.value != 'None':
            self.check_is_region(value)

    def is_region(self, input_string):
        if self.valid_regions is None:
            ec2_client = boto3.client('ec2', region_name='eu-west-1')
            response = ec2_client.describe_regions()
            self.valid_regions = [r['RegionName'] for r in response['Regions']]
        return input_string in self.valid_regions

    def check_is_region(self, value):
        if not self.is_region(value):
            raise DefaultRegionConfigParameter.InvalidRegionInConfigException('{r} is not a region'.format(
                r=str(value))
            )

    class InvalidRegionInConfigException(DefaultConfigParameter.InvalidConfigException):
        def __init__(self, reason):
            self.reason = reason


class DefaultBoolConfigParameter(DefaultConfigParameter):
    # noinspection PyDefaultArgument
    def __init__(self, name, value, description, possible_values=[]):
        super(DefaultBoolConfigParameter, self).__init__(name, value, description, possible_values)
        self.value_checks = [DefaultBoolConfigParameter.check_is_bool]
        self.type = 'bool'

    @staticmethod
    def is_bool(value):
        str_value = str(value)
        if str_value.lower() == 'false' or str_value.lower() == 'true':
            return True
        else:
            return False

    @staticmethod
    def check_is_bool(value):
        if not DefaultBoolConfigParameter.is_bool(value):
            raise DefaultBoolConfigParameter.InvalidBoolInConfigException('{v} is not a bool'.format(v=str(value)))

    class InvalidBoolInConfigException(DefaultConfigParameter.InvalidConfigException):
        def __init__(self, reason):
            self.reason = reason


class GlobalConfigParametersReader:
    def __init__(self, config_file='global_config_parameters.json'):
        self.config_file = config_file
        self.cli_arguments_to_process = None
        self.parameter_key_being_processed = None
        self.unprocessed_arguments = []
        self.config_parameters = self.get_default_config_parameters()

    def get_default_config_parameters(self):
        default_config_parameters = {}
        with open(self.config_file, 'r') as config_file_pointer:
            config = json.load(config_file_pointer)

        for key in config.keys():
            possible_values = config[key].get('possibleValues')
            if possible_values == '':
                possible_values = None
            default_config_parameters[key] = ConfigParameterFactory.make_default_config_parameter(
                key,
                config[key].get('value'),
                config[key].get('description'),
                possible_values
            )

        return default_config_parameters

    def get_default_config_key_values(self):
        global config_parameters
        for key, value in GlobalConfigParametersReader.get_key_value_dict(self.get_default_config_parameters()).items():
            config_parameters[key] = value
        return config_parameters

    def get_config_key_values_updated_with_cli_args(self, argv):
        self.get_default_config_parameter_updated_with_cli_args(argv)
        global config_parameters
        for key, value in GlobalConfigParametersReader.get_key_value_dict(self.config_parameters).items():
            config_parameters[key] = value
        return config_parameters

    def check_unprocessed_parameters(self):
        counter = 1
        if 's3ConfigFile' not in self.config_parameters or self.config_parameters['s3ConfigFile'].get_value() == 'None':
            self.config_parameters['s3ConfigFile'] = ConfigParameterFactory.make_config_parameter(
                self.config_parameters['s3ConfigFile'],
                self.unprocessed_arguments[counter].lower()
            )
            counter += 1

        if 'region' not in self.config_parameters or self.config_parameters['region'].get_value() == 'None':
            self.config_parameters['region'] = ConfigParameterFactory.make_config_parameter(
                self.config_parameters['region'],
                self.unprocessed_arguments[counter].lower()
            )
            counter += 1

        if len(self.unprocessed_arguments) != counter:
            raise GlobalConfigParametersReader.ParsingException(unprocessed_arguments=self.unprocessed_arguments)

    @staticmethod
    def get_key_value_dict(config_parameters_dict):
        result = {}
        for key in config_parameters_dict.keys():
            result[key] = config_parameters_dict[key].get_value()
        return result

    def get_default_config_parameter_updated_with_cli_args(self, argv):
        if len(self.unprocessed_arguments) > 0:
            return self.config_parameters
        self.cli_arguments_to_process = argv

        while len(self.cli_arguments_to_process) > 0:
            cli_flag = self.cli_arguments_to_process.pop(0)
            if self.is_a_parameter_key_being_processed():
                if cli_flag.startswith('--') \
                        or (self.is_parameter_being_processed_a_bool() and
                            (cli_flag.lower() != 'false' and cli_flag.lower() != 'true')):
                    self.process_parameter_without_value()
                else:
                    self.process_parameter_with_value(cli_flag)
                    continue

            if cli_flag.startswith('--'):
                parameter_key_name = GlobalConfigParametersReader.get_key_for_cli_flag(cli_flag)
                if parameter_key_name in self.config_parameters.keys():
                    self.parameter_key_being_processed = parameter_key_name
                else:
                    self.unprocessed_arguments.append(cli_flag)
            else:
                self.unprocessed_arguments.append(cli_flag)

        if self.is_a_parameter_key_being_processed():
            self.process_parameter_without_value()

        self.check_unprocessed_parameters()

        return self.config_parameters

    def is_parameter_being_processed_a_bool(self):
        return self.config_parameters[self.parameter_key_being_processed].type == 'bool'

    def is_a_parameter_key_being_processed(self):
        return self.parameter_key_being_processed is not None

    def process_parameter_without_value(self):
        default_config_parameter = self.config_parameters[self.parameter_key_being_processed]
        self.config_parameters[self.parameter_key_being_processed] = ConfigParameterFactory.make_config_parameter(
            default_config_parameter,
            'True'
        )
        self.parameter_key_being_processed = None

    def process_parameter_with_value(self, cli_flag):
        config_parameter_being_processed = self.config_parameters[self.parameter_key_being_processed]
        self.config_parameters[self.parameter_key_being_processed] = ConfigParameterFactory.make_config_parameter(
            config_parameter_being_processed,
            cli_flag.lower()
        )

        self.parameter_key_being_processed = None

    @staticmethod
    def get_key_for_cli_flag(cli_flag):
        result = ''
        if cli_flag.startswith('--'):
            cli_flag = cli_flag[2:]
        else:
            return None

        next_is_upper = False
        for letter in cli_flag:
            if letter == '-' and next_is_upper:
                return None
            if letter == '-':
                next_is_upper = True
                continue

            if next_is_upper:
                result += letter.upper()
                next_is_upper = False
            else:
                result += letter
        return result

    class ParsingException(Exception):
        # noinspection PyDefaultArgument
        def __init__(self, unprocessed_arguments=[]):
            super(GlobalConfigParametersReader.ParsingException, self).__init__()
            self.args = unprocessed_arguments
