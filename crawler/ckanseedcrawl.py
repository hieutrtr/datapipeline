import json, requests
import _thread
from datetime import datetime

def fetchBulkPackagesList():
    url = "https://datasets.seed.nsw.gov.au/api/3/action/package_list"
    response = requests.get(url)
    slice_len = 100
    temp_list = []
    packages = response.json()["result"]
    print("number of package {}".format(len(packages)))
    for i, package_name in enumerate(packages):
        if (i+1)%slice_len == 0:
            yield temp_list
            temp_list = []
        temp_list.append(package_name)
    yield temp_list

def getPackageMetadata(package_name):
    url = "https://datasets.seed.nsw.gov.au/api/3/action/package_show?id=" + package_name
    response = requests.get(url)
    return response.json()

def getPackagesMetadata(postfix, packages_list):
    print("packages_list number - {0}".format(postfix))
    print("packages_list length - {0}".format(len(packages_list)))
    partition = []
    for package_name in packages_list:
        data = getPackageMetadata(package_name)
        print("packages_data - {0}".format(package_name))
        partition.append(data)
    with open("seed_"+str(postfix)+".json", 'w') as outfile:
        json.dump(partition, outfile)

postfix_number = 0
for packages_list in fetchBulkPackagesList():
    _thread.start_new_thread(getPackagesMetadata, (postfix_number, packages_list))
    postfix_number+=1

while 1:
    pass