"""Native guard regression checks; no hypervisor or guest image required."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tools', type=Path, required=True)
    args = parser.parse_args()
    suffix = '.exe' if os.name == 'nt' else ''
    probe = args.tools.resolve() / ('guard-probe' + suffix)
    fixture = args.tools.resolve() / ('guard-fixture' + suffix)
    for mode, timeout, cancel, expected in [
        ('policy', 2000, None, 'completed'),
        ('stdout', 2000, None, 'output_limit'),
        ('stderr', 2000, None, 'output_limit'),
        ('loop', 150, None, 'timeout'),
        ('loop', 2000, 80, 'cancelled'),
    ]:
        command = [str(probe), str(fixture), mode, str(timeout)]
        if cancel:
            command.append(str(cancel))
        result = subprocess.run(command, capture_output=True, text=True, timeout=15, check=True)
        receipt = json.loads(result.stdout)
        assert receipt['status'] == expected, receipt
        assert receipt['resources_reclaimed'], receipt
        assert max(receipt['captured_bytes']) <= 8192, receipt
        assert receipt['policy_receipt']['applied'], receipt
        print(f'{mode}/{expected}: passed')
    if os.name != 'nt':
        result = subprocess.run([str(args.tools.resolve() / 'fd-probe')], capture_output=True, text=True, timeout=60, check=True)
        receipt = json.loads(result.stdout)
        assert receipt['passed'] and receipt['native_launches'] == 100
        print('100 concurrent descriptor-inheritance regression launches: passed')


if __name__ == '__main__':
    main()
