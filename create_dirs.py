import argparse
from datetime import datetime, timedelta
from pathlib import Path

parser = argparse.ArgumentParser(
    description="Create log folders (YYYY/MM/DD) for N days into the future."
)
parser.add_argument(
    'num_days', type=int, help="Create folders for this many days"
)

num_days = parser.parse_args().num_days
dates = [datetime.now() + timedelta(days=i) for i in range(num_days)]

def fmt(n):
    return f'{n:02d}'

for date in dates:
    year, month, day = map(fmt, (date.year, date.month, date.day))
    p = Path.cwd().joinpath(year, month, day)
    p.mkdir(exist_ok=True, parents=True)
    p.parent.parent.chmod(0o2755)
    p.parent.chmod(0o2755)
    p.chmod(0o3777)
