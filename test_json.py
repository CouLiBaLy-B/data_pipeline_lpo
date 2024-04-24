import json

# Ouvrir le fichier json
with open('test.json', 'r') as f:
    data = json.load(f)

# Maintenant, 'data' est un dictionnaire contenant vos données json
print(len(data['results']))
