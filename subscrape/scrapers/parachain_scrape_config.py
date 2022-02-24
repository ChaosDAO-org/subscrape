import copy

class ParachainScrapeConfig:
    def __init__(self, config):
        self.filter = None
        self.processor_name = None
        self.skip = False
        self.include_batch_calls = False
        self._set_config(config)

    def _set_config(self, config):
        if type(config) is list:
            return

        filter_conditions = config.get("_filter", None)
        if filter_conditions is not None:
            self.filter = self._filter_factory(filter_conditions)

        processor_name = config.get("_processor", None)
        if processor_name is not None:
            self.processor_name = processor_name

        skip = config.get("_skip", None)
        if skip is not None:
            self.skip = skip

        include_batch_calls = config.get("_include_batch_calls", None)
        if include_batch_calls is not None:
            self.include_batch_calls = include_batch_calls

    # creates a config that can be nested to lower layers
    def create_inner_config(self, config):
        result = copy.deepcopy(self)
        result._set_config(config)
        return result

    # returns true if the extrinsic should be skipped because it hits a filter condition
    def _filter_factory(self, conditions):
        if conditions is None:
            return None
        def filter(extrinsic):
            for group in conditions:
                for key in group:
                    if key not in extrinsic:
                        return True
                    actual_value = extrinsic[key]
                    predicates = group[key]
                    for predicate in predicates:
                        if "<" in predicate:
                            value = predicate["<"]
                            if actual_value < value:
                                continue
                            else:
                                return True
            return False
        return filter