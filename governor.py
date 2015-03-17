import copy
import inspect
from collections import defaultdict


class LimitConfigError(Exception):
    """Base class for exceptions in this module."""
    pass


class Governor(object):
    _RULES = []

    @classmethod
    def init(cls, config):
        cls._RULES = LimiterParser.parse_rules(config)

    def __init__(self, func=None, identifier=None):
        # self._submit_metric = func
        # self._submit_metric_arg_names = inspect.getargspec(func)[0]
        # self.instance_id = identifier
        self._rules = copy.deepcopy(self._RULES)

    def set(self, func):
        self._submit_metric = func
        self._submit_metric_arg_names = inspect.getargspec(func)[0]

    def _name_args(self, arg_list, kwargs):
        named_args = kwargs.copy()
        for i, arg_value in enumerate(arg_list):
            arg_name = self._submit_metric_arg_names[i + 1]
            named_args[arg_name] = arg_value
        return named_args

    def _check(self, args):
        # Check all
        return all(r.check(args) for r in self._rules)

    def __call__(self, *args, **kw):
        # Shortcut when no rules are defined
        if not self._rules:
            return self._submit_metric(*args, **kw)

        # Really dirty trick -> to improve
        named_args = self._name_args(args, kw)

        # Extract argument dict
        if self._check(named_args):
            return self._submit_metric(*args, **kw)

    def flush(self):
        pass

    def get_status(self):
        # Can be wrapped around get_metric method
        for r in self._rules:
            r.get_status()


class LimiterParser(object):
    # Context and metric scope limiters
    _CONTEXT_SCOPES = frozenset(['metric_name', 'instance', 'check'])
    _METRIC_NAME_SCOPES = frozenset(['instance', 'check'])

    # Constant keys
    _CONTEXT_KEYS = ['hostname', 'tags']
    _METRIC_NAME_KEY = ['metric_name']

    @staticmethod
    def parse_rules(config):
        """
        :param config: agent configuration
        :type config: dictionnary
        """
        # Process 'limit_contexts_by'
        rules = [ContextsLimiter(r['scope'], r['limit'])
                 for r in config.get('limit_contexts_by', [])]

        # Process 'limit_metric_name_number'
        extra_limit = config.get('limit_metric_name_number')
        if extra_limit:
            rules.append(MetricNamesLimiter(extra_limit['scope'], extra_limit['limit']))

        # TODO Process filter metric_name
        return rules

    # def submit_metric(self, name, value, mtype, tags=None, hostname=None,
    #                             device_name=None, timestamp=None, sample_rate=1):


# TODO
# Add a filter class
class Limiter(object):
    """
    A generic limiter
    """
    _SCOPES = []
    _TO_LIMIT = []

    def __init__(self, scope, limit):
        self._scope = scope
        self._to_scope_key, self._to_limit_key = self._extract_to_keys(scope)
        self._contexts_by_key = defaultdict(set)    # Hash storage
        self._counter_limit = limit                 # Actual limit
        self._blocked = 0                           # Blocked metrics counter

    @classmethod
    def _extract_to_keys(cls, scope):
        """
        :param scope: scope where the rule applies
        :type scope: string tuple or singleton
        """
        # Filter scope and cast as a tuple
        scope = scope if isinstance(scope, tuple) else (scope,)
        scope = tuple(s for s in scope if s in cls._SCOPES)

        return lambda x: tuple(x.get(k) for k in scope), \
            lambda x: tuple(x.get(k) for k in cls._TO_LIMIT)

    def check(self, *args, **kw):
        scope_key = self._to_scope_key(*args, **kw)
        limit_key = self._to_limit_key(*args, **kw)

        if limit_key in self._contexts_by_key[scope_key]:
            return True
        else:
            contexts = self._contexts_by_key[scope_key]
            contexts.add(limit_key)
            return len(contexts) <= self._counter_limit

    def get_status(self):
        pass


class ContextsLimiter(Limiter):
    """
    A limiter for contexts
    """
    _SCOPES = frozenset(['name', 'instance', 'check'])
    _TO_LIMIT = ('hostname', 'tags')


class MetricNamesLimiter(Limiter):
    """
    A limiter for metric names
    """
    _SCOPES = frozenset(['instance', 'check'])
    _TO_LIMIT = ('name',)
