import sqlite3
from typing import List

from git import Repo, InvalidGitRepositoryError

from errors import GitError


def get_all_commits(folder: str) -> List[str]:
    try:
        repo = Repo(folder)
        if repo.active_branch.is_valid():
            return [commit.hexsha for commit in repo.iter_commits()]
        else:
            return []
    except InvalidGitRepositoryError:
        raise GitError(f'No git repository in {folder}')


def get_previous_commit(db: str) -> str:
    connection = sqlite3.connect(db)
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS commits
                            (hash text, processed integer)''')
    connection.commit()
    cursor.execute('''SELECT hash FROM commits
                    ORDER BY processed DESC LIMIT 1''')
    result = cursor.fetchone()
    return result[0] if result is not None else ''
