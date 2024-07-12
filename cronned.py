import requests, json, glob, os
import dotenv
from queries import getReq, getOne
import pandas as pd

dotenv.load_dotenv()
username = os.getenv("API_username")
password = os.getenv("API_password")
URL = os.getenv("URL")

regenerate_answers = True

KW = [
    "digital",
    "data",
    "data engineering",
    "software developer",
    "machine learning",
    "artificial intelligence",
    "data management",
    "information management",
    "digital consultancy",
]
for kw in KW:
    kwn = kw.replace(" ", "-")
    for k in range(2):
        FILENAME = "/home/kelu/projets/mm_jobs/data/lists/list_" + kwn + "_" + str(k) + ".json"
        r = requests.post(
            URL,  # save the result to examine later
            auth=(username, password),  # auth
            json=getReq(kw, 20, k * 20),
        )  # no need to json.dumps or add the header manually!
        kw = kw.replace(" ", "-")
        if not r.text == "Unauthorized":
            with open(FILENAME, "w") as f:
                f.write(r.text)


IDs = []
for file in glob.glob("/home/kelu/projets/mm_jobs/data/lists/*.json"):
    # print(file)
    with open(file, "r") as f:
        t = f.read()
    if not t == "Unauthorized":
        D = json.loads(t)
        for x in D["hits"]["hits"]:
            IDs.append(x["_source"]["pageVersionId"])
            # print(x["_source"]["title"])
print(len(IDs), len(set(IDs)))


for k in IDs:
    REQ = getOne(k)
    FILENAME = "/home/kelu/projets/mm_jobs/data/jobs/job_" + str(k) + ".json"
    if not os.path.exists(FILENAME):
        print("Processing ", k)
        r = requests.post(
            URL,  # save the result to examine later
            auth=(username, password),  # you can pass this without constructor
            json=REQ,
        )  # no need to json.dumps or add the header manually!
        if not r.text == "Unauthorized":
            with open(FILENAME, "w") as f:
                f.write(r.text)


IDs = []
F = glob.glob("/home/kelu/projets/mm_jobs/data/jobs/*.json")
print(len(F))
for file in F:
    with open(file, "r") as f:
        t = f.read()
    if not t == "Unauthorized":
        D = json.loads(t)
        for x in D["hits"]["hits"]:
            IDs.append(x["_source"])


def getSkill(x):
    x = x.lower()
    if "data" in x:
        return "Data"
    elif "information management" in x:
        return "Information Management"
    elif "digital" in x:
        return "Digital"
    else:
        return ""


df = pd.DataFrame(IDs)[
    [
        "jobRef",
        "pageVersionId",
        "contentPageId",
        "title",
        "pageText",
        "publishedDate",
        "sector",
        "discipline",
        "jobSector",
    ]
]
df["Digital"] = df.sector.apply(lambda x: "Digital" in x)
df["skill"] = df.title.apply(lambda x: getSkill(x))
df.to_excel("/home/kelu/projets/mm_jobs/outputs/digital_jds.xlsx")
df.to_parquet("/home/kelu/projets/mm_jobs/outputs/digital_jds.parquet.gzip", compression="gzip")
df
