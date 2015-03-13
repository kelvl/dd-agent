import unittest

from governor import Governor, Rule, RuleParser


class MockMetricAggregator(object):
    """a MockClass for tests"""
    def __init__(self, footprint=None):
        self.submit_metric = Governor(self.submit_metric, footprint)

    def submit_metric(self, metric_name):
        return True


class GovernorTestCase(unittest.TestCase):
    LIMIT_METRIC_NB = {
        'limit_metric_name_number': {
            'scope': 'instance',
            'limit': 2
        }
    }

    NO_LIMIT = {}

    def test_no_check_contamination(self):
        """
        No cross contamination between != metric aggregators
        """
        # Governor.init(self.LIMIT_METRIC_NB)

        # m1 = MockMetricAggregator()
        # m2 = MockMetricAggregator()

        # self.assertTrue(m1.submit_metric(instance='firstInstance'))
        # self.assertTrue(m1.submit_metric(instance='firstInstance'))
        # self.assertTrue(m1.submit_metric(instance='firstInstance') is None)  # Blocked !

        # self.assertTrue(m2.submit_metric(instance='firstInstance'))  # Not blocked

    def test_no_instance_contamination(self):
        """
        Instance footprint is correctly being updated
        """
        instance_footprint = [0]
        Governor.init(self.LIMIT_METRIC_NB)

        m1 = MockMetricAggregator(instance_footprint)
        self.assertTrue(m1.submit_metric("metric.name"))
        self.assertTrue(m1.submit_metric("metric.name"))
        self.assertTrue(m1.submit_metric("metric.name") is None)  # Blocked !

        # Update instance_footprint
        instance_footprint.append(1)
        self.assertTrue(m1.submit_metric("metric.name"))

    def test_empty_conf(self):
        """
        Always accept when no rule is specified
        """
        Governor.init(self.NO_LIMIT)

        m1 = MockMetricAggregator()

        for x in xrange(1, 100):
            self.assertTrue(m1.submit_metric(metric_name="metric.name"))


class RuleTestCase(unittest.TestCase):
    def test_rule(self):
        """
        Test the rule logic
        """
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
            'scope': 'instance',
            'limit': 10
        }
    }

    NO_CONFIG = {}

    def test_rule_parser(self):
        """
        Test parsing logic
        """

        # No config
        self.assertTrue(RuleParser.parse_rules(self.NO_CONFIG) == [])

        # Incorrect config

        # Correct config
        rules = RuleParser.parse_rules(self.LIMIT_CONFIG)
        self.assertTrue(len(rules) == 4)

        metric_args = {
            'metric_name': 'metric.name',
            'useless_arg': 'useless_value',
            'instance': 'instance_identifier',
            'check': 'my_check_name'
        }
        self.assertTrue(rules[0].to_key(metric_args) == ('metric.name',))
        self.assertTrue(rules[1].to_key(metric_args) == ('metric.name', 'instance_identifier'))
        self.assertTrue(rules[2].to_key(metric_args) == ('my_check_name',))
        self.assertTrue(rules[3].to_key(metric_args) == ('instance_identifier',))
