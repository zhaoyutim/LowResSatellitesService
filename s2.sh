#curl --request POST --url https://services.sentinel-hub.com/oauth/token --header "content-type: application/x-www-form-urlencoded" --data "grant_type=client_credentials&client_id=4bb2314e-4bbd-4f8c-9673-5e08e98d53b8" --data-urlencode "client_secret=Cj/b3)+1qW96tNL(r.mJ3lLRVzsNv?^I3sLFug{+"
curl -X POST \
  https://services.sentinel-hub.com/api/v1/process \
  -H 'Authorization: Bearer eyJraWQiOiJzaCIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI3Y2NlODg5NC01YmJiLTQzZDItOGFjNy01NzhhZjhmZDU1OWYiLCJhdWQiOiI0YmIyMzE0ZS00YmJkLTRmOGMtOTY3My01ZTA4ZTk4ZDUzYjgiLCJqdGkiOiI4N2M5YzViMi0zNmQzLTRmZjMtOWQzYy1jYWRmNTc2YTgyNWEiLCJleHAiOjE2Mjc1NTg1MTUsIm5hbWUiOiJZdSBaaGFvIiwiZW1haWwiOiJ6aGFveXV0aW1AZ21haWwuY29tIiwiZ2l2ZW5fbmFtZSI6Ill1IiwiZmFtaWx5X25hbWUiOiJaaGFvIiwic2lkIjoiODBkYjA1YTctM2MzNS00OTkxLTk1MjctZTQ2YTY5MWViMjRhIiwiZGlkIjoxLCJhaWQiOiI4N2Y4ZWRjZi0yYzMyLTQ2YjYtOTQyYy04YWQyODY1NmVlZWYiLCJkIjp7IjEiOnsicmEiOnsicmFnIjoxfSwidCI6MTIwMDB9fX0.oWW-RCN7y2oNEKW_qy_ekZuMxV9fw361ansN_TreydiDreaAhjbTPO3p5jszSxb9J4YZdCZEI2TjdZyw0IdwJXntdhelBHrZNuHl4jv_zaSH9gAH1qesD5j1JtqV96squFepZ2xtOtiU_CELM2ghKEoXJPSo9AjxjIk4keFVQh8SMNX4MHhwQFWMRKVJS6pqDRNnH_IcefcNK_BUfNTdjYquwZ6t6uURs1imqx1HycO2hyeALms0w5nZulwATyAXiE2LKsXKzS5rgw5CzkR6sOEY9x_3ZHS370Zllc4r4hwxffwWis-DV04p0AEyBv0J21gvuo9GkEB3EmM9kqzDFw' \
  -F 'request={
    "input": {
        "bounds": {
            "properties": {
                "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
            },
            "bbox": [
                13.822174072265625,
                45.85080395917834,
                14.55963134765625,
                46.29191774991382
            ]
        },
        "data": [
            {
                "type": "sentinel-2-l2a",
                "dataFilter": {
                    "timeRange": {
                        "from": "2018-10-01T00:00:00Z",
                        "to": "2018-12-31T00:00:00Z"
                    }
                }
            }
        ]
    },
    "output": {
        "width": 512,
        "height": 512
    }
}' \
  -F 'evalscript=//VERSION=3

function setup() {
  return {
    input: ["B02", "B03", "B04"],
    output: {
      bands: 3,
      sampleType: "AUTO" // default value - scales the output values from [0,1] to [0,255].
    }
  }
}

function evaluatePixel(sample) {
  return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02]
}'
