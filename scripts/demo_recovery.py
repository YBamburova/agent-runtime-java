#!/usr/bin/env python3
"""Real PostgreSQL kill/restart demo. Requires the postgres-profile packaged JAR; no mock fallback."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--java', default='java')
    parser.add_argument('--jar', default='target/agent-runtime-java-0.1.0-SNAPSHOT.jar')
    parser.add_argument('--schema', default='recovery_' + uuid.uuid4().hex)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', args.schema):
        parser.error('Invalid schema name')
    if not os.environ.get('RELEASE_DB_URL'):
        parser.error('Set RELEASE_DB_URL to a dedicated PostgreSQL test database')
    environment = dict(os.environ, RELEASE_DB_SCHEMA=args.schema)
    command = [args.java, '-jar', str(Path(args.jar).resolve()), 'jobs']

    def run(*arguments):
        process = subprocess.run(command + list(arguments), env=environment, text=True,
                                 capture_output=True, timeout=15, check=False)
        if process.returncode:
            raise RuntimeError('Database CLI failed; check PostgreSQL setup and the postgres build profile')
        return process.stdout

    run('migrate')
    job = str(uuid.UUID(run('submit', 'crash-' + uuid.uuid4().hex, 'healthy', '2000', '3').strip()))
    with tempfile.TemporaryDirectory(prefix='investigator-recovery-') as directory:
        first_log = Path(directory) / 'first.log'
        second_log = Path(directory) / 'second.log'
        first = second = None
        try:
            with first_log.open('w') as output:
                first = subprocess.Popen(command + ['work', '--pause-before-commit'], env=environment,
                                         stdout=output, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 20
            while f'EXECUTED {job} attempt=1' not in first_log.read_text():
                if first.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('First worker did not reach the crash window')
                time.sleep(0.05)  # Bounded process-observation polling, not a correctness-test sleep.
            first.kill()  # SIGKILL: no shutdown hooks and no result commit.
            first.wait(timeout=5)
            with second_log.open('w') as output:
                second = subprocess.Popen(command + ['work'], env=environment, stdout=output, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 35
            while True:
                status = run('status', job)
                header = status.splitlines()[0]
                if 'state=SUCCEEDED' in header:
                    break
                if second.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('Replacement worker did not recover the job before the bound')
                time.sleep(0.1)
            if ('attempts=2' not in header or 'attempt=1 state=EXPIRED' not in status
                    or 'attempt=2 state=SUCCEEDED' not in status or status.count('RESULT attempt=2') != 1):
                raise AssertionError('Unexpected attempt history or persisted outcome')
            print(json.dumps({'schema': args.schema, 'job': job, 'attempts': 2, 'persisted_results': 1,
                              'first_attempt': 'EXPIRED', 'replacement': 'SUCCEEDED'}))
            print('External investigation/synthesis ran again; only the replacement persisted its result.')
        finally:
            for process in (first, second):
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
    # Keep the generated schema for inspection; never drop a user-selected database.


if __name__ == '__main__':
    main()
