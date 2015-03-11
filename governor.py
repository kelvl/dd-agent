from collections import defaultdict


class Governor(object):
    def __init__(self, func):
        self.func = func
        self.counter = 0
        self._rules = []

    def increment(self):
        self.counter += 1

    def __call__(self, *args, **kw):
        # Main work
        print "Processing metric"
        print "Args " + str(args)
        print "Kwargs " + str(kw)
        self.increment()
        print "Counter value " + str(self.counter)
        return self.func(*args, **kw)


class LimitConfigError(Exception):
    """Base class for exceptions in this module."""
    pass


class RuleParser(object):
    _CONTEXT_SCOPES = frozenset(['metric_name', 'instance', 'check'])
    _METRIC_NAME_SCOPES = frozenset(['instance', 'check'])

    @staticmethod
    def _scope_to_key(scope, contexts=True):
        """
        :param scope:
        :type scope: string tuple or singleton
        """
        scopes = RuleParser._CONTEXT_SCOPES if contexts else RuleParser._METRIC_NAME_SCOPES
        return lambda x: tuple(x.get(k) for k in scope if k in scopes)

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
        limit_metric_name_number = config.get('limit_metric_name_number')
        if limit_metric_name_number:
            rules.append(Rule(_._scope_to_key(r['scope'], False), r['limit']))

        return rules

    # def submit_metric(self, name, value, mtype, tags=None, hostname=None,
    #                             device_name=None, timestamp=None, sample_rate=1):


class Rule(object):
    def __init__(self, mtrc_to_ctxt_key, cnter_limit):
        self.to_key = mtrc_to_ctxt_key       # From metric returns hash key
        self.contexts_by_key = defaultdict(int)       # Hash storage
        self._counter_limit = cnter_limit            # Actual limit

    def check(self, *args, **kw):
        key = self.to_key(*args, **kw)
        self.contexts_by_key[key] += 1
        return self.contexts_by_key[key] <= self._counter_limit
