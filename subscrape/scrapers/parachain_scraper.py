__author__ = 'Tommi Enenkel @alice_und_bob'

from email.policy import strict
import logging
import string
from subscrape.apis.subscan_wrapper import SubscanWrapper


# A generic scraper for parachains
class ParachainScraper:
    """Scrape a substrate-based (non-EVM) chain for transactions/accounts of interest."""

    def __init__(self, api):
        self.logger = logging.getLogger(__name__)
        self.api: SubscanWrapper = api

    async def scrape(self, operations, chain_config) -> list:
        """Performs all the operations it was given by determining the operation and then calling the corresponding 
        method.

        :param operations: A dict of operations and it's subdicts
        :type operations: dict
        :param chain_config: the `ScrapeConfig` to bubble down configuration properties
        :type chain_config: ScrapeConfig
        :return: A list of scraped items
        """
        items = []

        for operation in operations:
            if operation.startswith("_"):
                continue

            if operation == "extrinsics":
                modules = operations[operation]
                new_items = await self.scrape_module_calls(modules, chain_config, self.api.fetch_extrinsic_metadata)
            elif operation == "extrinsics-list":
                extrinsics_list = operations[operation]
                new_items = await self.api.fetch_extrinsics(extrinsics_list)
            elif operation == "events":
                modules = operations[operation]
                new_items = await self.scrape_module_calls(modules, chain_config, self.api.fetch_event_metadata)
            elif operation == "events-list":
                events_list = operations[operation]
                new_items = await self.api.fetch_events(events_list)
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")
                exit

            items.extend(new_items)
        
        return items

    async def scrape_module_calls(self, modules, chain_config, fetch_function) -> list:
        """
        Scrapes all module calls that belong to the list of accounts.

        :param modules: dict of extrinsic modules to look for, like `system`, `utility`, etc
        :type modules: dict
        :param chain_config: the `ScrapeConfig`
        :type chain_config: ScrapeConfig
        :param fetch_function: the method to call to scrape extrinsics vs events etc
        :type fetch_function: function
        :return: the scraped items
        :rtype: list
        """
        items = []
        extrinsic_config = chain_config.create_inner_config(modules)

        # if we want to scrape all extrinsics, modules is None. In that case, we just set it to a list containing None
        if modules is None:
            modules = [None]

        for module in modules:
            # ignore metadata
            if (type(module) is string or type(module) is str) and module.startswith("_"):
                continue

            # if we want to scrape all extrinsics, we set call to None. Otherwise, take list of calls from the module
            if module is None:
                calls = None
            else:
                calls = modules[module]

            module_config = extrinsic_config.create_inner_config(calls)
            
            # if we want to scrape all calls, calls is None. In that case, we just set it to a list containing None
            if calls is None:
                calls = [None]

            for call in calls:
                # ignore metadata
                if (type(call) is string or type(call) is str) and call.startswith("_"):
                    continue

                # deduce config
                if type(calls) is dict:
                    call_config = module_config.create_inner_config(calls[call])
                else:
                    call_config = module_config

                # config wants us to skip this call?
                if call_config.skip:
                    self.logger.info(f"Config asks to skip {module} {call}")
                    continue

                # go
                new_items = await fetch_function(module, call, call_config)
                items.extend(new_items)
        return items
