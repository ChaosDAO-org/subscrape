__author__ = 'Tommi Enenkel @alice_und_bob'

import logging
from substrateinterface.utils import ss58
from subscrape.db.subscrape_db import SubscrapeDB


# A generic scraper for parachains
class ParachainScraper:
    """Scrape a substrate-based (non-EVM) chain for transactions/accounts of interest."""

    def __init__(self, db, api):
        self.logger = logging.getLogger("ParachainScraper")
        self.db: SubscrapeDB = db
        self.api = api

    def scrape(self, operations, chain_config):
        """Performs all the operations it was given by determining the operation and then calling the corresponding 
        method.
        
        :param operations: A dict of operations and it's subdicts
        :type operations: dict
        :param chain_config: the `ScrapeConfig` to bubble down configuration properties
        :type chain_config: ScrapeConfig
        """

        for operation in operations:
            if operation.startswith("_"):
                continue

            if operation == "extrinsics":
                modules = operations[operation]
                self.scrape_module_calls(modules, chain_config, self.fetch_extrinsics)
            elif operation == "events":
                modules = operations[operation]
                self.scrape_module_calls(modules, chain_config, self.fetch_events)
            elif operation == "transfers":
                accounts = operations[operation]
                self.scrape_transfers(accounts, chain_config)
            else:
                self.logger.error(f"config contained an operation that does not exist: {operation}")            
                exit

    def scrape_transfers(self, accounts, chain_config):
        """
        Scrapes all transfers that belong to the list of accounts.
        
        :param accounts: A dict of accounts on their names
        :type accounts: dict
        :param chain_config: the `ScrapeConfig`
        :type chain_config: ScrapeConfig
        """
        accounts_config = chain_config.create_inner_config(accounts)

        for account in accounts:
            # ignore metadata
            if account.startswith("_"):
                continue
            
            # deduce config
            if type(account) is dict:
                account_config = accounts_config.create_inner_config(accounts[account])
            else:
                account_config = accounts_config

            # config wants us to skip this call?
            if account_config.skip:
                self.logger.info(f"Config asks to skip account {account}")
                continue

            self.fetch_transfers(account, account_config)

    def fetch_transfers(self, account, call_config):
        """
        Fetches the transfers for a single account and writes them to the db.

        :param account: The account to scrape
        :type account: str
        :param call_config: The call_config which has the filter set
        :type call_config: ScrapeConfig
        """

        # normalize to Substrate address
        public_key = ss58.ss58_decode(account)
        substrate_address = ss58.ss58_encode(public_key, ss58_format=42)

        self.db.set_active_transfers_account(substrate_address)

        self.logger.info(f"Fetching transfers for {substrate_address} from {self.api.endpoint}")

        method = "/api/scan/transfers"

        body = {"address": substrate_address}
        self.api.iterate_pages(
            method,
            self.db.write_transfer,
            list_key="transfers",
            body=body,
            filter=call_config.filter
            )

        self.db.flush_transfers()

    def scrape_module_calls(self, modules, chain_config, fetch_function):
        """
        Scrapes all module calls that belong to the list of accounts.

        :param modules: dict of extrinsic modules to look for, like `system`, `utility`, etc
        :type modules: dict
        :param chain_config: the `ScrapeConfig`
        :type chain_config: ScrapeConfig
        :param fetch_function: the method to call to scrape extrinsics vs events etc
        :type fetch_function: function
        """
        extrinsic_config = chain_config.create_inner_config(modules)

        for module in modules:
            # ignore metadata
            if module.startswith("_"):
                continue

            calls = modules[module]
            module_config = extrinsic_config.create_inner_config(calls)
            
            for call in calls:
                # ignore metadata
                if call.startswith("_"):
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
                fetch_function(module, call, call_config)

    def fetch_extrinsics(self, module, call, config):
        """
        Scrapes all extrinsics matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        """
        extrinsics_storage = self.db.storage_manager_for_extrinsics_call(module, call)
        if config.digits_per_sector is not None:
            extrinsics_storage.digits_per_sector = config.digits_per_sector

        self.logger.info(f"Fetching extrinsic {module}.{call} from {self.api.endpoint}")

        method = "/api/scan/extrinsics"

        body = {"module": module, "call": call}
        self.api.iterate_pages(
            method,
            extrinsics_storage.write_item,
            list_key="extrinsics",
            body=body,
            filter=config.filter
            )

        extrinsics_storage.flush_sector()

    def fetch_events(self, module, call, config):
        """
        Scrapes all events matching the specified module and call (like `utility.batchAll` or `system.remark`)

        :param module: extrinsic module to look for, like `system`, `utility`, etc
        :type module: str
        :param call: extrinsic module's specific 'call' or method, like system's `remark` call.
        :type call: str
        :param config: the `ScrapeConfig`
        :type config: ScrapeConfig
        """
        extrinsics_storage = self.db.storage_manager_for_events_call(module, call)
        if config.digits_per_sector is not None:
            extrinsics_storage.digits_per_sector = config.digits_per_sector

        self.logger.info(f"Fetching events {module}.{call} from {self.api.endpoint}")

        method = "/api/scan/events"

        body = {"module": module, "call": call}
        self.api.iterate_pages(
            method,
            extrinsics_storage.write_item,
            list_key="events",
            body=body,
            filter=config.filter
            )

        extrinsics_storage.flush_sector()
"""

    def fetch_addresses(self):
        assert(len(self.addresses) == 0)

        file_path = self.db_path + "addresses.json"
        if os.path.exists(file_path):
            self.logger.warn(f"{file_path} already exists. Skipping.")
            return

        self.logger.info("Fetching accounts from " + self.endpoint)

        method = "/api/v2/scan/accounts"
        url = self.endpoint + method

        self.api.iterate_pages(url, self.process_account, list_key="list")

        file_path = self.db_path + "addresses.json"
        payload = json.dumps(self.addresses)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()


        def process_account(self, account):
        account_display = account["account_display"]
        address = account_display["address"]
        self.addresses.append(address)
        return True


"""