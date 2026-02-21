import subprocess
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_git_commit() -> str | None:
    """
    Get the current git commit hash (short form).
    Appends '-dirty' if there are uncommitted changes.
    
    Returns:
        Git commit hash like "0d59ec6" or "0d59ec6-dirty", or None if not in a git repo.
    """
    try:
        commit = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        status = subprocess.check_output(
            ['git', 'status', '--porcelain'],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL
        ).decode().strip()
        
        if status:
            commit += '-dirty'
        
        return commit
    except Exception:
        return None
