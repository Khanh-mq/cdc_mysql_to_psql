from ast import arg
from random import choice
import argparse  , pathlib , sys 
from datetime import date, datetime
from tarfile import data_filter
import pandas as pd


BASES ={
    "yellow" : "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet" ,
}

def month_range(start , end):
    s  = datetime.strptime(start , "%Y-%m")
    e  = datetime.strptime(end , "%Y-%m")
    cur =  datetime(s.year , s.month , 1)
    out = []
    while cur <= e:
        out.append((cur.year , cur.month))
        if cur.month == 12:
            cur = datetime(cur.year + 1 , 1 , 1)
        else:
            cur = datetime(cur.year , cur.month + 1 , 1)
    return out


def main():
    ap =  argparse.ArgumentParser()
    ap.add_argument("--start" , type=str , required=True , help="Start month in YYYY-MM format")
    ap.add_argument("--end" , type=str , required=True , help="End month in YYYY-MM format")
    ap.add_argument('--color' , choices=['yellow' , 'green'] , type=str , required=True , help="Taxi color")
    ap.add_argument('--outdir' , default="data/raw", help="Output directory")
    ap.add_argument('--force' , action='store_true' , help="Force re-download")
    ap.add_argument('--month' , type=str , help="Comma separated list of months to process in YYYY-MM format")
    args = ap.parse_args()


    outdir = pathlib.Path(args.outdir) ; outdir.mkdir(parents=True , exist_ok=True)
    months =  [args.month] if args.month else [f'{y}-{m:02d}' for (y,m) in month_range(args.start , args.end)]

    for ym in months:
        year , month = ym.split('-')
        year , month = int(year) , int(month)
        url = BASES[args.color].format(year=year , month=month)
        outpath = outdir / f"{args.color}_tripdata_{year}-{month:02d}.parquet"
        if outpath.exists() and not args.force:
            print(f"Skipping {outpath} since it already exists")
            continue 
        print(f"Downloading {url} to {outpath}")
        df = pd.read_parquet(url, engine="pyarrow")
        df.to_parquet(outpath)
        print(f"Wrote {len(df)} rows to {outpath}")
if __name__ == "__main__":
    sys.exit(main())

