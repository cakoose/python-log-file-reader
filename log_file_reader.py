import contextlib
from   dataclasses import dataclass
import os

@dataclass
class State:
    inode: int
    offset: int
    at_line_start: bool

    def to_jsonable(self):
        if self.at_line_start:
            return (self.inode, self.offset)
        else:
            return (self.inode, self.offset, False)

    @classmethod
    def from_jsonable(cls, j):
        if not isinstance(j, (list, tuple)):
            raise ValueError('expecting list or tuple, got {!r}'.format(j))

        if len(j) == 2:
            at_line_start = True
            pass
        elif len(j) == 3:
            at_line_start = j[2]
            if at_line_start != False:
                raise ValueError('if third element is present, it must be false, got {!r}'.format(at_line_start))
        else:
            raise ValueError('expecting 2 or 3 elements, got {!r}'.format(j))

        inode, offset = j[0], j[1]
        if not (isinstance(inode, int) and inode > 0):
            raise ValueError('element 1 (inode) must be a positive integer, got {!r}'.format(inode)) 
        if not (isinstance(offset, int) and offset >= 0):
            raise ValueError('element 2 (offset) must be a non-negative integer, got {!r}'.format(offset)) 

        return cls(inode, offset, at_line_start)

class CouldNotContinueException(Exception):
    """
    We couldn't figure out how to resume from the previous state.

    In your situation, it may be acceptable to display a warning message (e.g. "may
    have missed some log lines") and try again with state=None.
    """
    pass

def read(log_path, initial_read_limit, state):
    """
    initial_read_limit: If state is None, then we're starting from scratch.
        Pass None to start from the beginning of the log file.
        Otherwise pass in the number of bytes *before* the end of the log file to
        start.  We'll start from the next full line after that point.
    state: The first time, pass None.  For subsequent calls, pass in the State value
        returned by the previous call.

    This returns a generator that:
    1. yields all the new log lines.
    2. returns (via StopIteration) the state value to use in subsequent calls.

    May raise:
    - FileNotFoundError
    - CouldNotContinueException: Based on the state you passed in, we couldn't resume
      reading lines where we left off.  Either the file inode is different, or the file
      was truncated.
    """
    with open(log_path, 'rb') as f:
        return (yield from _read_f(f, os.fstat(f.fileno()), initial_read_limit, state))

# Same as 'read' except the file is already opened and stat'ed
def _read_f(f, f_stat, initial_read_limit, state):
    if state is None:
        return (yield from _read_initial(f, f_stat, initial_read_limit))
    else:
        return (yield from _read_continue(f, f_stat, state))

def _read_initial(f, f_stat, limit):
    if limit is None:
        offset = 0
        at_line_start = True
    else:
        assert isinstance(limit, int) and limit >= 0
        offset = max(0, f_stat.st_size - limit)
        at_line_start = (offset == 0)

    return (yield from _read_from(f, f_stat, offset, at_line_start))

def _read_continue(f, f_stat, state):
    assert isinstance(state, State)
    if f_stat.st_ino != state.inode:
        raise CouldNotContinueException("inode mismatch; expecting {!r}, got {!r}".format(state.inode, f_stat.st_ino))
    if f_stat.st_size < state.offset:
        raise CouldNotContinueException("shorter than offset; offset={!r}, size={!r}".format(state.offset, f_stat.st_size))

    return (yield from _read_from(f, f_stat, state.offset, state.at_line_start))

def _read_from(f, f_stat, offset, at_line_start):
    f.seek(offset)

    if not at_line_start:
        line = f.readline()
        offset += len(line)
        if not line.endswith(b'\n'):
            return State(f_stat.st_ino, offset, False)

    while True:
        line = f.readline()
        if not line.endswith(b'\n'):
            break
        offset += len(line)
        yield line

    return State(f_stat.st_ino, offset, True)

class NeitherFileFoundException(Exception):
    """Neither log1_path or log2_path was available to read."""
    pass

@contextlib.contextmanager
def _open_if_found(*args, **kwargs):
    try:
        f1 = open(*args, **kwargs)
    except FileNotFoundError:
        f1 = None

    try:
        yield f1
    finally:
        if f1 is not None:
            f1.close()

def read_rotated(log1_path, log2_path, initial_read_limit, state):
    """
    log1_path: The path to the latest log file.
    log2_path: The path to the first rotated log file.  The log rotation process should
        create this file by mv'ing the log file to this path, so the inode is preserved.
    initial_read_limit: If state is None, then we're starting from scratch.
        Pass None to start from the beginning of the earliest log file available.
        Otherwise pass in the number of bytes *before* the end of the latest log file
        start.  We'll start from the next full line after that point.
    state: The first time, pass None.  For subsequent calls, pass in the State value
        returned by the previous call.

    This returns a generator that:
    1. yields all the new log lines.
    2. returns (via StopIteration) the state value to use in subsequent calls.

    May raise:
    - NeitherFileFoundException: Neither of the log files exist.  Even during rotation,
      at least one of them must always exist.
    - CouldNotContinueException: Based on the state you passed in, we couldn't resume
      reading lines where we left off.  Either the file inode is different, or the file
      was truncated.
    """
    with _open_if_found(log1_path, 'rb') as f1:
        with _open_if_found(log2_path, 'rb') as f2:
            if f1 is None and f2 is None:
                raise NeitherFileFoundException()
            return (yield from _read_rotated(f1, f2, initial_read_limit, state))

def _read_rotated(f1, f2, initial_read_limit, state):
    # If only one file is present, use that.
    if f1 is None or f2 is None:
        f = f1 or f2
        return (yield from _read_f(f, os.fstat(f.fileno()), initial_read_limit, state))

    f1_stat = os.fstat(f1.fileno())
    f2_stat = os.fstat(f2.fileno())
    # If both file objects refer to the same underlying file, it's probably because the
    # log was rotated in between us opening f1 and f2.  We can read either.
    if f1_stat.st_ino == f2_stat.st_ino:
        return (yield from _read_f(f1, f1_stat, initial_read_limit, state))

    if state is None:
        # If there's no limit for the initial read, read both files fully.
        if initial_read_limit is None:
            yield from _read_initial(f2, f2_stat, None)
            return (yield from _read_initial(f1, f1_stat, None))

        # If f1 is not big enough to satisfy initial_read_limit, read the additional bytes from f2.
        f2_limit = max(0, initial_read_limit - f1_stat.st_size)
        if f2_limit > 0:
            # TODO: Figure out what to do if f2 ends with a partial line.
            yield from _read_initial(f2, f2_stat, f2_limit)
            initial_read_limit = None
        return (yield from _read_initial(f1, f1_stat, initial_read_limit))

    if f1_stat.st_ino == state.inode:
        return (yield from _read_continue(f1, f1_stat, state))
    elif f2_stat.st_ino == state.inode:
        # TODO: Figure out what to do if f2 ends with a partial line.
        yield from _read_continue(f2, f2_stat, state)
        return (yield from _read_initial(f1, f1_stat, None))
    else:
        raise CouldNotContinueException("inode mismatch; expecting {!r}, got {!r} and {!r}".format(state.inode, f1_stat.st_ino, f2_stat.st_ino))
