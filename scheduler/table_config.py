import os
from functools import reduce
from typing import Dict, List, Set

from utils.file_utils import read_config


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
