import unittest

from governor import Governor, Limiter, LimiterParser, LimiterConfigError
from aggregator import MetricsAggregator


class MockMetricAggregator(MetricsAggregator):
    """a MockClass for tests"""
    def __init__(self):
        mgovernor = Governor()
        super(MockMetricAggregator, self).__init__("", governor=mgovernor)

    def submit_metric(self, name, value=42, mtype='g', tags=None, hostname=None,
                      device_name=None, timestamp=None, sample_rate=1):
        return True


class MockLimiter(Limiter):
    """docstring for MockLimiter"""
    _ATOMS = frozenset(['key1', 'key2', 'key3', 'key4', 'key5'])


class GovernorTestCase(unittest.TestCase):
    LIMIT_METRIC_NB = {
        'limiters': [{
            'scope': 'check',
            'selection': 'name',
            'limit': 1
        }]
    }

    NO_LIMIT = {}

    def test_aggregators_contamination(self):
        """
        No cross contamination between != metric aggregators
        """
        Governor.init(self.LIMIT_METRIC_NB)

        self.assertTrue(len(Governor._LIMITERS) == 1)

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


class LimiterTestCase(unittest.TestCase):
    @staticmethod
    def generate_metric(v1, v2):
        """
        Helper to return a metric with
        """
        return {
            'key1': v1,
            'key2': v2,
        }

    def test_limit(self):
        """
        Check incoming metrics against the limit set
        """
        limiter = MockLimiter('key1', 'key2', 1)

        # Check limiter task
        self.assertTrue(limiter.check(self.generate_metric("scope1", "selection1")))
        self.assertFalse(limiter.check(self.generate_metric("scope1", "selection2")))
        self.assertTrue(limiter.check(self.generate_metric("scope1", "selection1")))

        # Check trace
        self.assertTrue(limiter._blocked_metrics == 1)

    def test_limiter_trace(self):
        """
        Generate a trace from submitted metrics
        """

        limiter = MockLimiter('key1', 'key2', 3)

        # Check trace definition
        definition = limiter.get_status()['definition']
        self.assertTrue(definition['scope'] == ('key1',))
        self.assertTrue(definition['selection'] == ('key2',))
        self.assertTrue(definition['limit'] == 3)

        # Submit metrics
        limiter.check(self.generate_metric("scope1", "selection1"))
        limiter.check(self.generate_metric("scope1", "selection2"))
        limiter.check(self.generate_metric("scope1", "selection3"))  # We reached the max
        limiter.check(self.generate_metric("scope1", "selection4"))  # Blocked !
        limiter.check(self.generate_metric("scope1", "selection2"))  # Has no effect

        limiter.check(self.generate_metric("scope2", "selection1"))
        limiter.check(self.generate_metric("scope2", "selection2"))

        # Check trace
        trace = limiter.get_status()['trace']
        self.assertTrue(trace['scope_cardinal'] == 2)
        self.assertTrue(trace['blocked_metrics'] == 1)
        self.assertTrue(trace['scope_overflow_cardinal'] == 1)
        self.assertTrue(trace['max_selection_scope'] == ("scope1",))
        self.assertTrue(trace['max_selection_cardinal'] == 3)

    def test_key_extractor(self):
        """
        Extract scope and selection keys from metrics
        """
        limiter1 = MockLimiter(('key1', 'key3'), ('key4', 'key5'), 1)
        limiter2 = MockLimiter('key1', 'key4', 1)

        metric_args = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
            'key4': 'value4',
            'key5': 'value5'
        }

        # Test _to_scope_key
        scope_key1, limit_key1 = limiter1._extract_metric_keys(metric_args)
        self.assertTrue(scope_key1 == ('value1', 'value3'))
        self.assertTrue(limit_key1 == ('value4', 'value5'))

        scope_key2, limit_key2 = limiter2._extract_metric_keys(metric_args)
        self.assertTrue(scope_key2 == ('value1',))
        self.assertTrue(limit_key2 == ('value4',))


class LimiterParserTestCase(unittest.TestCase):
    LIMIT_CONFIG = {
        'limiters': [
            {
                'scope': 'name',
                'selection': 'tags',
                'limit': 3
            },
            {
                'scope': ('name', 'instance'),
                'selection': 'tags',
                'limit': 5
            },
            {
                'scope': 'check',
                'selection': 'tags',
                'limit': 10
            },
            {
                'scope': 'instance',
                'selection': 'name',
                'limit': 10
            }
        ]
    }

    NO_SCOPE_CONFIG = {
        'limiters': [
            {
                # 'scope': 'name',
                'selection': 'tags',
                'limit': 3
            }
        ]
    }

    UNKOWN_SCOPE_CONFIG = {
        'limiters': [
            {
                'scope': ('name', 'unknown_scope'),
                'selection': 'tags',
                'limit': 3
            }
        ]
    }

    NO_CONFIG = {}

    def test_rule_parser(self):
        """
        Parse limiters
        """
        # No config
        self.assertTrue(LimiterParser.parse_limiters(self.NO_CONFIG) == [])

        # Incorrect config
        self.assertRaises(LimiterConfigError, LimiterParser.parse_limiters, self.NO_SCOPE_CONFIG)
        self.assertRaises(LimiterConfigError, LimiterParser.parse_limiters, self.UNKOWN_SCOPE_CONFIG)

        # Correct config
        rules = LimiterParser.parse_limiters(self.LIMIT_CONFIG)
        self.assertTrue(len(rules) == 4)
