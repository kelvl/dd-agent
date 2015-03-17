import unittest

from governor import Governor, Limiter, LimiterParser
from aggregator import MetricsAggregator


class MockMetricAggregator(MetricsAggregator):
    """a MockClass for tests"""
    # def __init__(self):
    #     self.governor = Governor(self.submit_metric)
    #     self.submit_metric = self.governor.call

    # @Governor
    # def submit_metric(self, name, value, mtype, tags=None, hostname=None,
    #                   device_name=None, timestamp=None, sample_rate=1):
    #     return True

    def __init__(self):
        mgovernor = Governor()
        super(MockMetricAggregator, self).__init__("", governor=mgovernor)

    def submit_metric(self, name):
        return True


class MockLimiter(Limiter):
    """docstring for MockLimiter"""
    _SCOPES = frozenset(['key1', 'key2', 'key3'])
    _TO_LIMIT = ('key4', 'key5')


class GovernorTestCase(unittest.TestCase):
    LIMIT_METRIC_NB = {
        'limit_metric_name_number': {
            'scope': 'check',
            'limit': 1
        }
    }

    NO_LIMIT = {}

    def test_aggregators_contamination(self):
        """
        No cross contamination between != metric aggregators
        """
        Governor.init(self.LIMIT_METRIC_NB)

        self.assertTrue(len(Governor._RULES) == 1)

        m1 = MockMetricAggregator()
        m2 = MockMetricAggregator()

        self.assertTrue(m1.submit_metric('my_metric'))
        self.assertFalse(m1.submit_metric('another_metric'))  # Blocked !
        self.assertTrue(m1.submit_metric('my_metric'))    # Not blocked !

        self.assertTrue(m2.submit_metric('another_metric'))  # Not blocked

    def test_instance_contamination(self):
        pass

    def test_empty_conf(self):
        """
        Always accept when no rule is specified
        """
        Governor.init(self.NO_LIMIT)

        m1 = MockMetricAggregator()

        for x in xrange(1, 100):
            self.assertTrue(m1.submit_metric(name='my_metric'))

    def test_name_args(self):
        """
        Properly name arguments
        """
        def myfunction(self, arg1, arg2, arg3):
            pass
        m_governor = Governor()
        m_governor.set(myfunction)
        self.assertTrue(
            m_governor._name_args([1, 2, 3], {}) == {'arg1': 1, 'arg2': 2, 'arg3': 3})
        self.assertTrue(
            m_governor._name_args([1], {'arg2': 2, 'arg3': 3}) == {'arg1': 1, 'arg2': 2, 'arg3': 3})

    # def test_output(self):
    #     Governor.init(self.LIMIT_METRIC_NB)
    #     m1 = MockMetricAggregator()

    #     m1.submit_metric('my_metric')
    #     m1.submit_metric('another_metric')
    #     m1.submit_metric('my_metric')

    #     import pdb
    #     pdb.set_trace()
    #     m1.submit_metric.print_summary()


class LimiterTestCase(unittest.TestCase):
    def test_limit(self):
        """
        Test the limiter check logic
        """
        metric1 = {
            'key1': 'value',
            'key4': 'value',
            'key5': 'value'

        }
        metric2 = {
            'key1': 'value',
            'key4': 'other_value',
            'key5': 'value'

        }

        limiter = MockLimiter('key1', 1)
        self.assertTrue(limiter.check(metric1))
        self.assertFalse(limiter.check(metric2))
        self.assertTrue(limiter.check(metric1))

    def test_key_extractor(self):
        """
        Test the key extraction logic
        """
        limiter1 = MockLimiter(('key1', 'wrongkey', 'key3'), 1)
        limiter2 = MockLimiter('key1', 1)

        metric_args = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
            'key4': 'value4',
            'key5': 'value5'
        }
        # Test _to_scope_key
        self.assertTrue(limiter1._to_scope_key(metric_args) == ('value1', 'value3'))
        self.assertTrue(limiter2._to_scope_key(metric_args) == ('value1',))

        # Test _to_limit_key
        self.assertTrue(limiter1._to_limit_key(metric_args) == ('value4', 'value5'))
        self.assertTrue(limiter2._to_limit_key(metric_args) == ('value4', 'value5'))


class LimiterParserTestCase(unittest.TestCase):
    LIMIT_CONFIG = {
        'limit_contexts_by': [
            {
                'scope': 'name',
                'limit': 3
            },
            {
                'scope': ('name', 'instance'),
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
        self.assertTrue(LimiterParser.parse_rules(self.NO_CONFIG) == [])

        # Incorrect config

        # Correct config
        rules = LimiterParser.parse_rules(self.LIMIT_CONFIG)
        self.assertTrue(len(rules) == 4)
