import os
from functools import reduce
from typing import Dict, List, Set

import networkx as nx

from utils.errors import ConfigFieldError
from utils.file_utils import read_config, find_processor, load_ddl_query
from utils.table import Table


def parse_table_config(full_table_name: str, views_path: str) -> Dict:
    schema, table = full_table_name.split(".")

    global_config = read_config(os.path.join(views_path, "global.conf"))
    schema_config_outside = read_config(os.path.join(views_path, f"{schema}.conf"))
    schema_config_inside = read_config(
        os.path.join(views_path, schema, f"{schema}.conf")
    )
    table_config_outside = read_config(
        os.path.join(views_path, f"{schema}.{table}.conf")
    )
    table_config_inside = read_config(os.path.join(views_path, schema, f"{table}.conf"))

    merged = {
        **global_config,
        **schema_config_outside,
        **schema_config_inside,
        **table_config_outside,
        **table_config_inside,
    }

    for key, value in merged.items():
        if value in ("null", "None", ""):
            merged[key] = None

    if "grant_select" in merged:
        configs = [
            global_config,
            schema_config_outside,
            schema_config_inside,
            table_config_outside,
            table_config_inside,
        ]
        merged["grant_select"] = parse_permissions("grant_select", configs)

    return merged


def parse_permissions(key: str, configs: List[Dict]) -> str:
    permissions = [config.get(key) for config in configs]
    merged = reduce(merge_permissions, permissions, [])
    return ", ".join(sorted(merged))


def merge_permissions(acc: Set, value: str) -> Set:
    if value is None or value == "":
        return acc

    new_values = value.replace(" ", "").split(",")

    if "+" not in value and "-" not in value:
        return set(new_values)

    to_add = {val[1:] for val in new_values if val[0] == "+"}
    to_remove = {val[1:] for val in new_values if val[0] == "-"}

    return acc.union(to_add).difference(to_remove)


def build_table_configs(graph: nx.DiGraph, views_path: str) -> List[Table]:
    nodes = dict(graph.nodes(data=True))
    return [
        Table(
            table,
            data["contents"],
            data["interval"],
            parse_table_config(table, views_path),
        )
        for table, data in nodes.items()
    ]


def check_config_fields(tables: List[Table], views_path: str):
    for table in tables:
        distkey, sortkey = table.config.get("distkey"), table.config.get("sortkey")
        if not distkey and not sortkey:
            continue

        processor = find_processor(table.name, views_path)

        if processor:
            ddl_query = load_ddl_query(views_path, table.name)
            if distkey and distkey not in ddl_query:
                raise ConfigFieldError(
                    f"Distkey {distkey} missing from create table query for {table.name}."
                )

            if sortkey and sortkey not in ddl_query:
                raise ConfigFieldError(
                    f"Sortkey {sortkey} missing from create table query for {table.name}."
                )

            continue

        if distkey and distkey not in table.query:
            raise ConfigFieldError(
                f"Distkey {distkey} missing from select query for {table.name}."
            )

        if sortkey and sortkey not in table.query:
            raise ConfigFieldError(
                f"Sortkey {sortkey} missing from select query for {table.name}."
            )
