from urllib import request
import certifi
import requests
import zipfile,io
import gzip



year="2020"
month="03"
day="22"
hour="21"
output_path="test.gz"



url=f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz"

print(url)
c=0
# r = requests.get(url,stream=True)
# with open(output_path, 'wb') as f:
#     for chunk in r.iter_content(chunk_size=1024): 
#         if chunk: # filter out keep-alive new chunks
#             f.write(chunk)
#             print(c)
#             c+=1

# with open("test","r") as f:
#     for line in f:
#         print(line)
#         c+=1
#         if c==100:
#             break

with gzip.open("test.gz","rb") as f:
    for line in f:
        print(line.decode("utf-8"))
        c+=1
        if c==100:
            break
