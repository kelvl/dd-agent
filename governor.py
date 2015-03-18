import copy
import inspect
from collections import defaultdict


class LimiterConfigError(Exception):
    """
    Error when parsing limiter
    """
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
    """
    A generic limiter
    """
    @staticmethod
    def parse_rules(config):
        """
        :param config: agent configuration
        :type config: dictionnary
        """
        # Process 'limit_contexts_by'
        limiters = [Limiter(r.get('scope'), r.get('selection'), r.get('limit'))
                    for r in config.get('limiters', [])]

        return limiters

    # def submit_metric(self, name, value, mtype, tags=None, hostname=None,
    #                             device_name=None, timestamp=None, sample_rate=1):


class Limiter(object):
    """
    A generic limiter
    """
    _ATOMS = frozenset(['name', 'instance', 'check', 'tags'])

    def __init__(self, scope, selection, limit=None):
        # Definition
        self._scope, self._selection = self._make_scope_and_selection(scope, selection)
        self._limit_cardinal = limit

        # Metric values extractor
        self._extract_metric_keys = self._extract_to_keys(self._scope, self._selection)

        # Limiter data storage
        self._selections_by_scope = defaultdict(set)

        # Trace
        self._blocked = 0

    @classmethod
    def _make_scope_and_selection(cls, scope, selection):
        """
        Check limiter `scope` and `selection` settings. Cast as a tuple and returns.

        :param scope: scope where the rule applies
        :type scope: string tuple or singleton

        :param selection: selection where the rule applies
        :type selection: string tuple or singleton
        """
        if not scope or not selection:
            raise LimiterConfigError("Limiters must contain a `scope` and a `selection`.")

        scope = scope if isinstance(scope, tuple) else (scope,)
        selection = selection if isinstance(selection, tuple) else (selection,)

        for s in scope:
            if s not in cls._ATOMS:
                raise LimiterConfigError("Unrecognized {0} within `scope`. `scope` must"
                                         " be a subset of {1}".format(s, cls._ATOMS))
        for s in selection:
            if s not in cls._ATOMS:
                raise LimiterConfigError("Unrecognized {0} within `selection`. `selection` must"
                                         " be a subset of {1}".format(s, cls._ATOMS))

        return scope, selection

    @classmethod
    def _extract_to_keys(cls, scope, selection):
        """
        Returns a function that extracts scope and selection values from a metric

        :param scope: scope where the rule applies
        :type scope: string tuple or singleton

        :param selection: selection where the rule applies
        :type selection: string tuple or singleton
        """
        return lambda x: (tuple(x.get(k) for k in scope), tuple(x.get(k) for k in selection))

    def check(self, *args, **kw):
        """
        Limiter main task.
        Check incoming metrics against the limit set, and returns a boolean

        :param *args: metric list parameters
        :param **kw: metric named parameters
        """
        scope_key, limit_key = self._extract_metric_keys(*args, **kw)

        if limit_key in self._selections_by_scope[scope_key]:
            return True
        else:
            contexts = self._selections_by_scope[scope_key]

            if len(contexts) >= self._limit_cardinal:
                self._blocked += 1
                return False

            contexts.add(limit_key)
            return True

    def get_status(self):
        """
        Returns limiter trace:
        `scope_cardinal`            -> Number of scopes registred
        `scope_select`              -> Scope with the maximum of selections registred
        `selection_max_cardinal`    -> Maximum number of selections registred for a scope
        `blocked`                   -> Number of blocked metrics
        """
        scope_cardinal = len(self._selections_by_scope)
        blocked = self._blocked

        scope

        for scope, selections in self._selections_by_scope.iteritems():
            pass



        pass
