class MaterializationError(Exception):
    """Basic exception for materialization"""


class MaterializationGraphError(MaterializationError):
    """Basic exception for graph-related errors"""


class NotADAGError(MaterializationGraphError):
    """When a views dependency graph is not a DAG"""


class RootsWithoutIntervalError(MaterializationGraphError):
    """When a views dependency graph is not a DAG"""

