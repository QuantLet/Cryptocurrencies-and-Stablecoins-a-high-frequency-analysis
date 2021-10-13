import json
import shutil
import sys
import os
import tarfile
import datetime
import pandas as pd

import boto3

BUCKET_NAME = 'kaiko-delivery-qfinlab-polimi'


def build_filter_months(from_month, to_month):
    from_year, from_month = from_month.split('-')
    to_year, to_month = to_month.split('-')
    from_date = datetime.datetime(int(from_year), int(from_month), 1)
    to_date = datetime.datetime(int(to_year), int(to_month), 1)
    assert from_date <= to_date
    dates = pd.date_range(from_date, to_date + datetime.timedelta(weeks=5), freq='M').tolist()
    return [date.strftime('/%Y/%m/') for date in dates]


def read_order_books_tars(file_name, s3):
    s3.download_file(BUCKET_NAME, f'scripts/{file_name}', file_name)
    with open(file_name) as f:
        lines = [line.rstrip() for line in f]
    os.remove(file_name)
    return lines


def main(pair, from_month, to_month):
    requested_months = build_filter_months(from_month, to_month)

    session = boto3.session.Session()
    s3 = session.client('s3')

    # read file with all tar file names
    tars = read_order_books_tars('order-books-tars.txt', s3)

    # filter according to pair, from_month and to_month
    tars = [tar for tar in tars if f'_{pair}_' in tar and any([(month in tar) for month in requested_months])]

    # loop through the tars
    for tar in tars:
        # read tar from s3 bucket
        tar_file_name = os.path.basename(tar)
        print(tar_file_name)
        s3.download_file(BUCKET_NAME, tar, tar_file_name)

        # extract locally
        tar_file = tarfile.open(tar_file_name)
        tar_directory, _ = os.path.splitext(tar_file_name)
        print(tar_directory)
        for name in tar_file.getnames():
            print(name)
            tar_file.extract(name)
            # upload extracted files to s3 bucket
            s3.upload_file(name, BUCKET_NAME, f'data/provider-data/order-book/unpacked/{pair}/{name}')
            # delete file after uploading
            os.remove(name)

        # close and remove after uploading all files in tar
        tar_file.close()
        os.remove(tar_file_name)


if __name__ == "__main__":
    parameters = json.loads(sys.argv[1])
    main(parameters['pair'],
         parameters['from_month'],
         parameters['to_month'])
