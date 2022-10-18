from pprint import pprint

import requests
from pydap.client import open_url
from pydap.cas.urs import setup_session
import numpy as np
from datetime import datetime, timedelta
import json
if __name__ == '__main__':
    link = 'https://ladsweb.modaps.eosdis.nasa.gov/api/v1/files/product=VNP02IMG&collection=5200&dateRanges=2021-07-13..2021-07-14&areaOfInterest=x-129.5y56.2,x-110.4y31.7'
    my_headers = {
        "X-Requested-With": "XMLHttpRequest",
        'Authorization': 'Bearer emhhb3l1dGltOmVtaGhiM2wxZEdsdFFHZHRZV2xzTG1OdmJRPT06MTYzMzk0NzU0NTphZmRlYWY2MjE2ODg0MjQ5MTEzNmE3MTE4MzZkOWYxYjg3MWQzNWMz'}
    response_vnp02 = requests.get(link, headers=my_headers)
    result = response_vnp02.json()
    pprint(result)

