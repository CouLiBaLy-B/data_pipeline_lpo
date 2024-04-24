import requests
import pandas as pd
import matplotlib.pyplot as plt

# URL de l'API
api_url = "https://biodiv-sports.fr/api/v2/sensitivearea/"


def data_extract(api_url):
    # Récupération des données depuis l'API
    response = requests.get(api_url)
    res = response.json()
    res = res["results"]
    # print(type(res[0]))
    return res

def data_transformation(res: list):
    dic = {}
    for _res in res:
        dic["create_datetime"] = _res["create_datetime"]
        dic["id"] = _res["id"]
        dic["description"] = _res["description"]
        dic["name"] = _res["name"]["fr"]
        dic["structure"] = _res["structure"]
        dic["elevation"] = _res["elevation"]
        dic["provider"] = _res["provider"]
    df = pd.DataFrame.from_dict(dic)
    return df, len(dic.keys())
  
res = data_extract(api_url)

print(data_transformation(res))
