import unittest
# from nose.plugins.attrib import attr

from governor import Rule, RuleParser


class RuleTestCase(unittest.TestCase):
    def test_rule(self):
        new_rule = Rule(lambda *args: tuple(args), 3)
        self.assertTrue(new_rule.check('arg1', 'arg2', 'arg3'))
        self.assertTrue(new_rule.check('arg1', 'arg2', 'arg3'))
        self.assertTrue(new_rule.check('arg1', 'arg2', 'arg3'))
        self.assertFalse(new_rule.check('arg1', 'arg2', 'arg3'))
        self.assertTrue(new_rule.check('arg1'))


class RuleParserTestCase(unittest.TestCase):
    LIMIT_CONFIG = {
        'limit_contexts_by': [
            {
                'scope': 'metric_name',
                'limit': 3
            },
            {
                'scope': ('metric_name', 'instance'),
                'limit': 5
            },
            {
                'scope': 'check',
                'limit': 10
            }
        ],
        'limit_metric_name_number': {
            'scope': 'instance'
        }
    }

    NO_CONFIG = {}

    def test_rule_parser(self):
        # No config
        self.assertTrue(RuleParser.parse_rules(self.NO_CONFIG) == [])

        # Incorrect config

        # Correct config
        rules = RuleParser.parse_rules(self.LIMIT_CONFIG)
        self.assertTrue(len(rules) == 4)

        fake_args = {
            'metric_name': 'metric.name',
            'useless_arg': 'useless_value',
            'instance': 'instance_identifier',
            'check': 'my_check_name'
        }
        self.assertTrue()

