class MaterializationError(Exception):
    """Basic exception for materialization"""


class MaterializationGraphError(MaterializationError):
    """Basic exception for graph-related errors"""


class NotADAGError(MaterializationGraphError):
    """When a views dependency graph is not a DAG"""


class RootsWithoutIntervalError(MaterializationGraphError):
    """When a views dependency graph is not a DAG"""


class GitError(MaterializationError):
    """When we couldn’t fetch new commits"""


class TestsFailedError(MaterializationError):
    """When tests failed"""


class TableNotFoundError(MaterializationError):
    """No table with this name found in config db"""


class TableCreationError(MaterializationError):
    """Couldn’t create a table in Redshift"""


class ProcessorNotFoundError(MaterializationError):
    """Could’t load a processor"""


class RedshiftUploadError(MaterializationError):
    """Could’t copy a table into Redshift"""


class RedshiftConnectionError(MaterializationError):
    """Couldn‘t connect to Redshift"""
