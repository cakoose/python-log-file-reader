import json
import os
import pytest
import shutil

import log_file_reader as lfr

class TestRead:
    def prep(self, tmp_path):
        log_path = tmp_path / 'log.txt'

        def run_read(state, initial_read_limit=None):
            return GeneratorTester(lfr.read(log_path, initial_read_limit, state))

        return log_path, run_read

    def test_basic(self, tmp_path):
        log_path, run_read = self.prep(tmp_path)

        # Write two full lines
        log = open(log_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'line 2\n')

        r = run_read(None)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        state = r.done()

        # Write one full line
        log.write(b'line 3\n')

        r = run_read(state)
        assert r.next() == b'line 3\n'
        state = r.done()

        # Write a partial line
        log.write(b'lin')

        r = run_read(state)
        state = r.done()

        # Finish the line, then a full line, then a partial line
        log.write(b'e 4\n')
        log.write(b'line 5\n')
        log.write(b'line 6')

        r = run_read(state)
        assert r.next() == b'line 4\n'
        assert r.next() == b'line 5\n'
        state = r.done()

        # Finish the last line
        log.write(b'\n')

        r = run_read(state)
        assert r.next() == b'line 6\n'

    def test_initial_read_limit(self, tmp_path):
        log_path, run_read = self.prep(tmp_path)

        # Write two full lines
        log = open(log_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'line 2\n')

        states = []

        r = run_read(state=None, initial_read_limit=7)
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=8)
        assert r.next() == b'line 2\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=13)
        assert r.next() == b'line 2\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=14)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        states.append(r.done())

        for state in states:
            r = run_read(state)
            r.done()

        log.write(b'line 3\n')

        for state in states:
            r = run_read(state)
            assert r.next() == b'line 3\n'
            r.done()


    def test_initial_read_limit_partial_line(self, tmp_path):
        log_path, run_read = self.prep(tmp_path)

        # Write a line and a half
        log = open(log_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'lin')

        r = run_read(state=None, initial_read_limit=0)
        state1 = r.done()

        r = run_read(state=None, initial_read_limit=3)
        assert r.done() == state1

        r = run_read(state=None, initial_read_limit=4)
        state2 = r.done()

        log.write(b'e 3\n')

        r = run_read(state1)
        r.done()

        r = run_read(state2)
        assert r.next() == b'line 3\n'
        r.done()

    def test_inode_change(self, tmp_path):
        log_path, run_read = self.prep(tmp_path)
        log_path_copy = tmp_path / 'log-copy.txt'

        log = open(log_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'line 2\n')
        log.close()

        r = run_read(None)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        state = r.done()

        r = run_read(state)
        state = r.done()

        # Overwrite the path with an identical copy, but with a different inode
        shutil.copy(log_path, log_path_copy)
        os.replace(log_path_copy, log_path)

        try:
            r = run_read(state)
            print(r.next())
            assert False, "expecting exception"
        except lfr.CouldNotContinueException:
            pass

    def test_truncate(self, tmp_path):
        log_path, run_read = self.prep(tmp_path)

        log = open(log_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'line 2\n')

        r = run_read(None)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        state = r.done()

        r = run_read(state)
        state = r.done()

        # Truncate the file, making it one byte shorter
        log.seek(-1, os.SEEK_END)
        log.truncate()
        log.close()

        try:
            r = run_read(state)
            print(r.next())
            assert False, "expecting exception"
        except lfr.CouldNotContinueException:
            pass

    def test_not_found(self, tmp_path):
        log_path = tmp_path / 'log.txt'

        def run_read(state):
            return GeneratorTester(lfr.read(log_path, None, state))

        try:
            r = run_read(None)
            print(r.next())
            assert False, "expecting exception"
        except FileNotFoundError:
            pass

class TestReadRotated:
    def prep(self, tmp_path):
        log1_path = tmp_path / 'log1.txt'
        log2_path = tmp_path / 'log2.txt'

        def run_read(state, initial_read_limit=None):
            return GeneratorTester(lfr.read_rotated(log1_path, log2_path, initial_read_limit, state))

        return log1_path, log2_path, run_read

    def test_basic(self, tmp_path):
        log1_path, log2_path, run_read = self.prep(tmp_path)

        log = open(log1_path, 'wb', buffering=0)
        log.write(b'line 1\n')
        log.write(b'line 2\n')

        r = run_read(None)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        state = r.done()

        # Rotate
        os.replace(log1_path, log2_path)
        log.write(b'line 3\n')

        r = run_read(state)
        assert r.next() == b'line 3\n'
        state = r.done()

        # Write a line more to the rotated file, then a line to the original.
        log.write(b'line 4\n')
        log.close()
        log = open(log1_path, 'wb', buffering=0)
        log.write(b'line 5\n')

        r = run_read(state)
        assert r.next() == b'line 4\n'
        assert r.next() == b'line 5\n'
        state = r.done()

        # Rotate, write a partial line to the original.
        os.replace(log1_path, log2_path)
        log.close()
        log = open(log1_path, 'wb', buffering=0)
        log.write(b'lin')
        
        r = run_read(state)
        state = r.done()

        # Write remaining line, rotate, write partial
        log.write(b'e 6\n')
        os.replace(log1_path, log2_path)
        log = open(log1_path, 'wb', buffering=0)
        log.write(b'lin')

        r = run_read(state)
        assert r.next() == b'line 6\n'
        state = r.done()

        log.write(b'e 7\n')

        r = run_read(state)
        assert r.next() == b'line 7\n'
        state = r.done()

    def test_not_found(self, tmp_path):
        log1_path, log2_path, run_read = self.prep(tmp_path)

        try:
            r = run_read(None)
            print(r.next())
            assert False, "expecting exception"
        except lfr.NeitherFileFound:
            pass

    def test_initial_read_limit(self, tmp_path):
        log1_path, log2_path, run_read = self.prep(tmp_path)

        with open(log2_path, 'wb', buffering=0) as log2:
            log2.write(b'line 1\n')
            log2.write(b'line 2\n')

        log = open(log1_path, 'wb', buffering=0)
        log.write(b'line 3\n')
        log.write(b'line 4\n')

        states = []

        r = run_read(state=None, initial_read_limit=7)
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=8)
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=13)
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=14)
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=15)
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=21)
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=22)
        assert r.next() == b'line 2\n'
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=27)
        assert r.next() == b'line 2\n'
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=28)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=29)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        r = run_read(state=None, initial_read_limit=None)
        assert r.next() == b'line 1\n'
        assert r.next() == b'line 2\n'
        assert r.next() == b'line 3\n'
        assert r.next() == b'line 4\n'
        states.append(r.done())

        for state in states:
            r = run_read(state)
            r.done()

        log.write(b'line 5\n')

        for state in states:
            r = run_read(state)
            assert r.next() == b'line 5\n'
            r.done()

class TestState:
    def test_manual(self):
        assert lfr.State(1, 0, True).to_jsonable() == (1, 0)
        assert lfr.State(1, 0, False).to_jsonable() == (1, 0, False)
        assert lfr.State(10, 100, True).to_jsonable() == (10, 100)
        assert lfr.State(10, 100, False).to_jsonable() == (10, 100, False)

        assert lfr.State.from_jsonable([1, 2]) == lfr.State(1, 2, True)
        assert lfr.State.from_jsonable([1, 2, False]) == lfr.State(1, 2, False)
        assert lfr.State.from_jsonable([10, 20]) == lfr.State(10, 20, True)
        assert lfr.State.from_jsonable([10, 20, False]) == lfr.State(10, 20, False)

    def test_errors(self):
        def check(j, message):
            with pytest.raises(ValueError) as ex:
                lfr.State.from_jsonable(j)
            assert str(ex.value) == message

        check({}, 'expecting list or tuple, got {}')
        check(12, 'expecting list or tuple, got 12')

        check([], 'expecting 2 or 3 elements, got []')
        check((), 'expecting 2 or 3 elements, got ()')
        check([1], 'expecting 2 or 3 elements, got [1]')
        check((2,), 'expecting 2 or 3 elements, got (2,)')

        check([1, 2, 3, 4], 'expecting 2 or 3 elements, got [1, 2, 3, 4]')
        check((1, 2, 3, 4), 'expecting 2 or 3 elements, got (1, 2, 3, 4)')

        check([1, 2, 3], 'if third element is present, it must be false, got 3')
        check([1, 2, None], 'if third element is present, it must be false, got None')
        check([1, 2, True], 'if third element is present, it must be false, got True')

        check([0, 2], 'element 1 (inode) must be a positive integer, got 0')
        check([1.2, 2], 'element 1 (inode) must be a positive integer, got 1.2')
        check(["hello", 2], 'element 1 (inode) must be a positive integer, got \'hello\'')

        check([1, -2, False], 'element 2 (offset) must be a non-negative integer, got -2')
        check([1, 1.2, False], 'element 2 (offset) must be a non-negative integer, got 1.2')
        check([1, "hello", False], 'element 2 (offset) must be a non-negative integer, got \'hello\'')

    def test_round_trip(self):
        def check(state):
            assert state == lfr.State.from_jsonable(state.to_jsonable())

        for inode in (1, 100, 100000):
            for offset in (0, 100, 100000):
                for at_line_start in (True, False):
                    state = lfr.State(inode=inode, offset=offset, at_line_start=at_line_start)
                    j = state.to_jsonable()
                    serialized = json.dumps(j)
                    loaded = json.loads(serialized)
                    assert state == state.from_jsonable(j)
                    assert state == state.from_jsonable(loaded)

class GeneratorTester:
    def __init__(self, gen):
        self._gen = gen
    def next(self):
        return next(self._gen)
    def done(self):
        try:
            v = next(self._gen)
        except StopIteration as e:
            return e.value
        raise AssertionError('expecting end, got another value: {!r}'.format(v))
