from DataCollector import DataCollector
import polars as pl
import json
import os

schema_file = "cfg/DataCollectorSchema.json"
config_file = "cfg/DataCollectorConfig.json"
queries_file = "cfg/Queries.json"
contract = "0xD0638b91bC6B301A0eEF5A109ED11cb30ed13bCE"

with open(queries_file) as f:
    queries = json.loads(f.read())


for key, query_info in queries.items():
    is_update = False
    dc = DataCollector(schema_file, config_file, query_info)
    query = {
        'start_block': 17855654,
        'end_block': 17855655,
        'rpc': dc.config["rpc_url"],
        'hex': True,
        'contract': query_info["contract_address"]
    }
    # Update json file with ABIs if not present
    if len(query_info["abi"]) == 0:
        queries[key]["abi"] = dc.abi
        with open(queries_file, 'w') as f:
            data = json.dumps(queries)
            f.write(json.dumps(queries))

    # Check if File exists - load and check blocks
    if os.path.isfile(query_info["output_path"]):
        is_update = True
        data = dc.load(query_info["output_path"])
        start_block = data.select(pl.col("block_number").last()).to_numpy()[0][0] + 1
        end_block = start_block + 10
        query["start_block"] = start_block
        query["end_block"] = end_block

    # 0xA43fe16908251ee70EF74718545e4FE6C5cCEc9f "0xD0638b91bC6B301A0eEF5A109ED11cb30ed13bCE"
    result = dc.collect(query)
    if result.shape[0] == 0:
        print("No events found")
        quit()
        
    decoded = dc.decode(result)
    if is_update:
        decoded = pl.concat([data, decoded], how="vertical")
    dc.save(decoded, query_info["output_path"])
    print(decoded)

    decoded.groupby("to").agg(pl.count(), pl.sum("amount1In"), pl.sum("amount0Out"))
