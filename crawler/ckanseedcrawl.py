import json, requests
from datetime import datetime

def getPackagesList():
    url = "https://datasets.seed.nsw.gov.au/api/3/action/package_list"
    response = requests.get(url)
    slice_len = 10
    temp_list = []
    for i, package_name in enumerate(response.json()["result"]):
        if (i+1)%slice_len == 0:
            yield temp_list
            temp_list = []
        temp_list.append(package_name)
    yield temp_list

def getPackageMetadata(package_name):
    url = "https://datasets.seed.nsw.gov.au/api/3/action/package_show?id=" + package_name
    response = requests.get(url)
    return response.json()

for packages_list in getPackagesList():
    print("packages_list - {0}".format(packages_list))
    partition = []
    for package_name in packages_list:
        data = getPackageMetadata(package_name)
        print("packages_data - {0}".format(data))
        partition.append(data)
    print("writing file ...")
    with open("seed_"+datetime.now().strftime("%m-%d-%Y-%H:%M:%S")+".json", 'w') as outfile:
        json.dump(partition, outfile)