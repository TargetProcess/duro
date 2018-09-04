import re


def is_table_used_in_query(table_name: str, query: str) -> bool:
    clean_query = remove_comments(query)
    schema, table = table_name.split(".")
    return bool(re.search(fr"\b\"?{schema}\"?\.\"?{table}\"?\b", clean_query))


def remove_comments(query: str) -> str:
    lines = query.split("\n")
    lines_without_comments = [
        line.split("--")[0] if "--" in line else line for line in lines if line
    ]
    return "\n".join(lines_without_comments)
