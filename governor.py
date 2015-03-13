import copy
from collections import defaultdict


class LimitConfigError(Exception):
    """Base class for exceptions in this module."""
    pass


class Governor(object):
    _RULES = []

    @classmethod
    def init(cls, config):
        cls._RULES = RuleParser.parse_rules(config)

    def __init__(self, func, instance_footprint):
        self.func = func
        self.instance_footprint = instance_footprint or [0]
        # Recursive copy
        self._rules = copy.deepcopy(self._RULES)

    def _check(self, **kw):
        # All rules pass
        return all(r.check(kw) for r in self._rules)

    def __call__(self, *args, **kw):
        # Main work
        # print "Processing metric"
        # print "Args " + str(args)
        # print "Kwargs " + str(kw)
        if self._check(instance=tuple(self.instance_footprint), **kw):
            return self.func(*args, **kw)


class RuleParser(object):
    # Context and metric scope limiters
    _CONTEXT_SCOPES = frozenset(['metric_name', 'instance', 'check'])
    _METRIC_NAME_SCOPES = frozenset(['instance', 'check'])

    @staticmethod
    def _scope_to_key(scope, contexts=True):
        """
        :param scope: rule scope
        :type scope: string tuple or singleton
        """
        _ = RuleParser
        available_scopes = _._CONTEXT_SCOPES if contexts else _._METRIC_NAME_SCOPES
        scope = scope if isinstance(scope, tuple) else (scope,)
        return lambda x: tuple(x.get(k) for k in scope if k in available_scopes)

    @staticmethod
    def parse_rules(config):
        """
        :param config: agent configuration
        :type scope: dictionnary
        """
        _ = RuleParser
        # Process 'limit_contexts_by'
        rules = [Rule(_._scope_to_key(r['scope']), r['limit'])
                 for r in config.get('limit_contexts_by', [])]

        # Process 'limit_metric_name_number'
        extra_limit = config.get('limit_metric_name_number')
        if extra_limit:
            rules.append(Rule(_._scope_to_key(extra_limit['scope'], False), extra_limit['limit']))

        return rules

    # def submit_metric(self, name, value, mtype, tags=None, hostname=None,
    #                             device_name=None, timestamp=None, sample_rate=1):


class Rule(object):
    def __init__(self, mtrc_to_ctxt_key, cnter_limit):
        self.to_key = mtrc_to_ctxt_key       # From metric returns hash key
        self._contexts_by_key = defaultdict(int)       # Hash storage
        self._counter_limit = cnter_limit            # Actual limit

    def check(self, *args, **kw):
        key = self.to_key(*args, **kw)
        self._contexts_by_key[key] += 1
        return self._contexts_by_key[key] <= self._counter_limit
