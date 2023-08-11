from web3 import Web3
import json, requests
import jsonschema
import cryo
import polars as pl

from typing import Any, Dict, cast, Union

from eth_typing import HexStr
from eth_utils import event_abi_to_log_topic
from hexbytes import HexBytes
from web3._utils.abi import get_abi_input_names, get_abi_input_types, map_abi_data
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from web3.contract import Contract

from abc import ABC


class DataCollector(ABC):
    def __init__(self, schema_file, config_file, query_info):
        self.config = self.validate_and_load_config(schema_file, config_file)
        self.abi = query_info["abi"]
        if len(query_info["abi"]) == 0:
            self.abi = self.get_abi(query_info["contract_address"])
        self.w3 = Web3(provider=Web3.HTTPProvider(self.config["rpc_url"]))
        self.contract = self.w3.eth.contract(address=query_info["contract_address"], abi=self.abi)
        self.decoder = EventLogDecoder(self.contract)
    
    def get_abi(self, contract_address):
        abi_endpoint = "https://api.etherscan.io/api?module=contract&action=getabi&address=" + contract_address + "&apikey=" + self.config["etherscan_api_key"] 
        abi = json.loads(requests.get(abi_endpoint).text)["result"]
        return abi



    @staticmethod
    def validate_and_load_config(schema_file, config_file):
        with open(schema_file) as f:
            schema = json.loads(f.read())
        with open(config_file) as f:
            cfg = json.loads(f.read())
        jsonschema.validate(cfg, schema)
        return cfg
    
    @staticmethod
    def collect(query):
        datatype = 'logs'
        #cryo.collect(datatype, **queries)
        results = cryo.collect(datatype, **query)
        return results
    
    def decode(self, data):
        # Decode Hex Data
        ab = data.with_columns(pl.struct(['topic0', 'topic1', 'topic2', 'data']).apply(lambda x: self.decoder.decode_event_input_polars(x)).alias("decoded")).unnest("decoded")
        # Transform to Ether (10^18)
        ab = ab.with_columns(pl.col("amount0In", "amount1Out","amount1In", "amount0Out", "reserve0", "reserve1") / 1000000000000000000)
        return ab

    @staticmethod
    def save(data, path):
        data.write_parquet(path)
        return
    
    @staticmethod
    def load(path):
        data = pl.read_parquet(path)
        return data

# See https://ethereum.stackexchange.com/questions/58912/how-do-i-decode-the-transactions-log-with-web3-py
class EventLogDecoder:
    def __init__(self, contract: Contract):
        self.contract = contract
        self.event_abis = [abi for abi in self.contract.abi if abi['type'] == 'event']
        self._sign_abis = {event_abi_to_log_topic(abi): abi for abi in self.event_abis}
        self._name_abis = {abi['name']: abi for abi in self.event_abis}
    
    def decode_event_input_polars(self, result, name: str = None) -> Dict[str, Any]:
        # type ignored b/c expects data arg to be HexBytes
        selector = HexBytes(result["topic0"])
        # Decide which method is used
        if name:
            func_abi = self._get_event_abi_by_name(event_name=name)
        else:
            func_abi = self._get_event_abi_by_selector(selector)

        # Sort data based on index true/false
        data = ""
        i = 1
        data_provided = False
        for input in func_abi["inputs"]:
            if input["indexed"]:
                data += result["topic"+str(i)][2:]
                i += 1
            elif not data_provided:
                data += result["data"][2:]
                data_provided = True

        params = HexBytes(data)
        
        names = get_abi_input_names(func_abi)
        types = get_abi_input_types(func_abi)

        decoded = self.contract.w3.codec.decode(types, cast(HexBytes, params))
        normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded)

        return dict(zip(names, normalized))

    def _get_event_abi_by_selector(self, selector: HexBytes) -> Dict[str, Any]:
        try:
            return self._sign_abis[selector]
        except KeyError:
            raise ValueError("Event is not presented in contract ABI.")

    def _get_event_abi_by_name(self, event_name: str) -> Dict[str, Any]:
        try:
            return self._name_abis[event_name]
        except KeyError:
            raise KeyError(f"Event named '{event_name}' was not found in contract ABI.")