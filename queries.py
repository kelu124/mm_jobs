
def getOne(ID):
    ID = str(ID)
    return {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "cultureId": 127
                    }
                },
                {
                    "term": {
                        "pageType": "7"
                    }
                },
                {
                    "term": {
                        "pageVersionId": ID
                    }
                }
            ],
        }
    },
    "size": 5,
    "from": 0,
    "track_scores": True,
    "sort": [
        "_score"
    ]
}


def getReq(QUERY,SIZE,FROM):
    return {
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "cultureId": 127
                    }
                },
                {
                    "term": {
                        "pageType": "7"
                    }
                },
                {
                    "multi_match": {
                        "query": QUERY,
                        "type": "phrase",
                        "fields": [
                            "pageText",
                            "keyWords",
                            "summaryText",
                            "jobSector",
                            "title",
                            "location",
                            "jobRef"
                        ]
                    }
                }
            ]
        }
    },
    "size": SIZE,
    "from": FROM,
    "track_scores": True,
    "sort": [
        "_score"
    ]
}
