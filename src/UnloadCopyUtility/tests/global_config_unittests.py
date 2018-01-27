import unittest
import copy
from global_config import GlobalConfigParametersReader, DefaultConfigParameter


class GlobalConfigUnittests(unittest.TestCase):
    def setUp(self):
        self.processable_parameters = ['--connection-pre-test', 'False', '--destinationTableAutoCreate']

    def test_flag_must_start_with_double_dash(self):
        self.assertIsNone(GlobalConfigParametersReader.get_key_for_cli_flag('-a_value'))

    def test_flag_must_not_have_double_dash_inside(self):
        self.assertIsNone(GlobalConfigParametersReader.get_key_for_cli_flag('--a--value'))

    def test_simple_flag(self):
        self.assertEqual('a', GlobalConfigParametersReader.get_key_for_cli_flag('--a'))

    def test_camel_case_flag(self):
        cli_flag = '--a-b'
        expected_result = 'aB'
        self.assertEqual(expected_result, GlobalConfigParametersReader.get_key_for_cli_flag(cli_flag))

    def test_camel_case_flag_multiple_letters(self):
        cli_flag = '--a-bc'
        expected_result = 'aBc'
        self.assertEqual(expected_result, GlobalConfigParametersReader.get_key_for_cli_flag(cli_flag))

    def test_global_config_reader_default_value(self):
        default_parameters = GlobalConfigParametersReader().get_default_config_parameters()
        expected_value = True
        returned_value = default_parameters['connectionPreTest'].get_value()
        self.assertEqual(expected_value, returned_value)

    def test_global_config_reader_overwritten_default_value(self):
        cli_params = ['application_name', 'config_file', 'eu-west-1', '--connection-pre-test', 'False']
        config_reader = GlobalConfigParametersReader()
        default_parameters = config_reader.get_config_key_values_updated_with_cli_args(cli_params)
        expected_value = False
        returned_value = default_parameters['connectionPreTest']
        self.assertEqual(expected_value, returned_value)

    def test_global_config_reader_overwritten_default_value_bool_without_explicit_value(self):
        cli_params = ['application_name', 'config_file', 'eu-west-1', '--destinationTableAutoCreate', '--connection-pre-test', 'False']
        config_reader = GlobalConfigParametersReader()
        default_parameters = config_reader.get_config_key_values_updated_with_cli_args(cli_params)
        expected_value = True
        returned_value = default_parameters['destinationTableAutoCreate']
        self.assertEqual(expected_value, returned_value)

    def test_global_config_reader_overwritten_default_value_bool_without_explicit_value_at_end(self):
        cli_params = ['application_name', 'config_file', 'eu-west-1', '--connection-pre-test', 'False', '--destinationTableAutoCreate']
        config_reader = GlobalConfigParametersReader()
        default_parameters = config_reader.get_config_key_values_updated_with_cli_args(cli_params)
        expected_value = True
        returned_value = default_parameters['destinationTableAutoCreate']
        self.assertEqual(expected_value, returned_value)

    def test_global_config_reader_overwritten_default_value_bool_without_explicit_value_at_end2(self):
        cli_params = ['application_name', 'config_file', 'eu-west-1', '--connection-pre-test', 'False', '--destinationTableAutoCreate']
        config_reader = GlobalConfigParametersReader()
        final_parameters = config_reader.get_config_key_values_updated_with_cli_args(cli_params)
        expected_value = True
        returned_value = final_parameters['destinationTableAutoCreate']
        self.assertEqual(expected_value, returned_value)
        self.assertFalse(final_parameters['connectionPreTest'])

    def test_global_config_reader_unprocessed_values(self):
        unprocessed_parameters = ['application_name', 'config_file', 'eu-west-1']
        full_args = copy.deepcopy(unprocessed_parameters)
        for parameter in self.processable_parameters:
            full_args.append(parameter)
        config_reader = GlobalConfigParametersReader()
        config_reader.get_config_key_values_updated_with_cli_args(full_args)
        self.assertEqual(unprocessed_parameters, config_reader.unprocessed_arguments)

    def test_global_config_reader_unknown_key_should_raise_parsing_exception(self):
        unprocessed_parameters = ['application_name', 'config_file', 'eu-west-1']
        full_args = copy.deepcopy(unprocessed_parameters)
        for parameter in self.processable_parameters:
            full_args.append(parameter)
        unknown_key = '--unknown-key'
        unprocessed_parameters.append(unknown_key)
        full_args.append(unknown_key)
        config_reader = GlobalConfigParametersReader()
        self.assertRaises(GlobalConfigParametersReader.ParsingException, config_reader.get_config_key_values_updated_with_cli_args, full_args)
        self.assertEqual(unprocessed_parameters, config_reader.unprocessed_arguments)

    def test_global_config_reader_invalid_region(self):
        unprocessed_parameters = ['application_name', 'config_file', '--region', 'europe-west-1']
        config_reader = GlobalConfigParametersReader()
        self.assertRaises(DefaultConfigParameter.InvalidConfigException, config_reader.get_config_key_values_updated_with_cli_args,unprocessed_parameters)

    def test_global_config_reader_valid_region(self):
        valid_region_name = 'us-east-2'
        unprocessed_parameters = ['application_name', 'config_file', '--region', valid_region_name]
        config_reader = GlobalConfigParametersReader()
        config_key_values = config_reader.get_config_key_values_updated_with_cli_args(unprocessed_parameters)
        self.assertEqual(valid_region_name, config_key_values['region'])

    def test_global_config_reader_valid_if_boolean_flag_is_used_and_followed_by_no_argument(self):
        valid_region_name = 'eu-west-1'
        unprocessed_parameters = ['redshift_unload_copy.py', '--log-level', 'debug', '--destination-table-auto-create', 'conf.json', 'eu-west-1']
        config_reader = GlobalConfigParametersReader()
        config_key_values = config_reader.get_config_key_values_updated_with_cli_args(unprocessed_parameters)
        self.assertEqual(valid_region_name, config_key_values['region'])