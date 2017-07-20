class MaterializationError(Exception):
    """Basic exception for materialization"""
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Materialization error'
        super().__init__(msg)


class SchedulerError(MaterializationError):
    """Basic exception for scheduler errors"""


class CreationError(MaterializationError):
    """Basic exception for scheduler errors"""
    def __init__(self, table, message):
        self.table = table
        self.message = message
        super().__init__(message)


class MaterializationGraphError(SchedulerError):
    """Basic exception for graph-related errors"""
    # def __init__(self):


class NotADAGError(SchedulerError):
    """When a views dependency graph is not a DAG"""


class RootsWithoutIntervalError(SchedulerError):
    """When a views dependency graph is not a DAG"""


class GitError(SchedulerError):
    """When we couldn’t fetch new commits"""


class TestsFailedError(CreationError):
    """When tests failed"""
    def __init__(self, table):
        super().__init__(table, f'Tests failed for `{table}`')


class TableNotFoundInDBError(CreationError):
    """No table with this name found in config db"""
    def __init__(self, table):
        super().__init__(table, f'`{table}` not found in db')


class TableNotFoundInGraphError(CreationError):
    """No table with this name found in config db"""
    def __init__(self, table):
        super().__init__(table, f'`{table}` not found in graph file')


class TableCreationError(CreationError):
    """Couldn’t create a table in Redshift"""
    def __init__(self, table, details=None):
        message = f'''*Select query failed for `{table}`* ```{details}```'''
        super().__init__(table, message)


class ProcessorNotFoundError(CreationError):
    """Could’t load a processor"""
    def __init__(self, processor):
        super().__init__(processor, f'Processor `{processor}` not found')


class RedshiftUploadError(CreationError):
    """Could’t copy a table into Redshift"""
    def __init__(self, table):
        super().__init__(table, f'Couldn’t upload data to S3 for `{table}`')


class RedshiftConnectionError(CreationError):
    """Couldn‘t connect to Redshift"""
    def __init__(self):
        super().__init__('Couldn’t connect to Redshift', 'Couldn’t connect to Redshift')
