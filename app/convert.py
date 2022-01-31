import base64
with open("websiteaccess.json", "rb") as f:
    data = f.read()
    encoded = base64.b64encode(data)
    print(encoded.decode('ascii'))
