import geopandas as gpd
import os
import json
import requests
from shapely.geometry import Point
import pyproj


def get_sharepoint_access_headers_through_client_id():
    client_id = "50d23ac1-8de9-4941-9399-004472826045"
    client_secret = "1Zo5LkdK4ZZfzuIXKO2o2FslnZBC5keyqqpEqW1YWEs="
    tenant_id = "a096cfba-db7b-4c9c-9506-d8e91da824ee"
    tenant = "greenlandscapingmalmo"
    client_id = client_id + '@'+tenant_id
    
    data = {
    'grant_type':'client_credentials',
    'resource': "00000003-0000-0ff1-ce00-000000000000/" + tenant + ".sharepoint.com@" + tenant_id, 
    'client_id': client_id,
    'client_secret': client_secret
}
    url = "https://accounts.accesscontrol.windows.net/tenant_id/tokens/OAuth/2"
    headers = {
    'Content-Type':'application/x-www-form-urlencoded'
}

    url = f"https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
    r = requests.post(url, data=data, headers=headers)
    json_data = json.loads(r.text)
    headers = {
    'Authorization': "Bearer " + json_data['access_token'],
    'Accept':'application/json;odata=verbose',
    'Content-Type': 'application/json;odata=verbose'
}
    return headers   

def replace_symbols(string):

        string = string.replace('å', 'a').replace('ä', 'a').replace('ö', 'o').replace('ü','u').replace('Å', 'A').replace('Ä', 'A').replace('Ö', 'O').replace('Ü','U')

        string = ''.join(c for c in string if c.isalnum())
        if string[0].isnumeric():
            string = "a"+string

        return string
    
def applicationdata_from_sharepoint():
        url = "https://greenlandscapingmalmo.sharepoint.com/sites/TrdexperternaApplikationer"+"/_api/web/lists/getbytitle('Geodata fordon')/items"
        
        js = requests.get(url,headers=get_sharepoint_access_headers_through_client_id()).json()['d']['results']
        # TODO: 
        # Add some logic to differentiate between one or more lists. (Probably by organization and make each input a list instead of a dictionary)
        return js
def get_all_active_organizations():
    js = applicationdata_from_sharepoint()
    setlist=[]
    for obj in js:
        if obj["Referensnamn"] not in setlist:
            setlist.append(obj["Referensnamn"])
    return setlist
    #print(json.dumps(js,indent=4,ensure_ascii=False))
    
class Abax:
    def __init__(self):
        self.__CLIENT_ID = "Zhc2xGwE7V6pQuJe4GtQGOoK4MQbaVsh"
        self.__CLIENT_SECRET="432prUMA1VtVB6JipLJrC2aO5Z4Xgthp"
        self.org_info = None
        self.namelist = []
        self.user = 'Green'
    def request_auth(self, scope):
        endpoint = "https://identity.abax.cloud/connect/token"
        body = f"grant_type=client_credentials&scope=open_api+{scope}&client_id={self.__CLIENT_ID}&client_secret={self.__CLIENT_SECRET}"
        response=requests.post(endpoint,data=body,headers={"Content-Type":"application/x-www-form-urlencoded"})
        return response.json()
    def request_equipment(self):
        auth = self.request_auth("open_api.equipment")
        endpoint = "https://api.abax.cloud/v2/equipment"
        headers = {"Authorization":"Bearer "+auth["access_token"]}
        rs = requests.get(endpoint,headers=headers)
        return rs
    def set_unique_name(self,fname):
        if fname in self.namelist:
            if "(" in fname[-3]:
                fname = fname[:-3] +f"({str(int(fname[-2])+1)})"
                print(fname) 
            else:
                fname = fname + "(1)"

        self.namelist.append(fname) 
        return fname.strip()
    
    
    def write_file_based_on_organization(self, geodf):
        orglist = get_all_active_organizations()
        for item in geodf.iterrows():
            if item[1]["Bolag"] not in orglist: continue
            fpath = os.path.join("data",os.path.join("publish",os.path.join("Green",replace_symbols(item[1]["Bolag"]))))
            fname = self.set_unique_name(item[1]["Namn"])
            if not os.path.exists(fpath):os.makedirs(fpath)
            gdf = gpd.GeoDataFrame([item[1].to_dict()])
            gdf.to_file(os.path.join(fpath,fname+".shp"))

    def create_organization_structure(self):
        df = self.get_equipment_dataframe()
        self.write_file_based_on_organization(df)
        return {"Success":"Successfully wrote shapefiles."}
    def get_equipment_dataframe(self):
        js = self.request_equipment().json()
        eqlist = []
        for item in js['items']:
            if "location" in item.keys() and "alias" in item.keys():
                lon,lat = item["location"]["longitude"],item["location"]["latitude"]
                lon,lat=self.transform_coordinates(lon,lat)
                eqlist.append({
                               "geometry":Point(lon,lat),"Abax_id":item["asset_id"],
                               "Namn":item["alias"],
                               "Bolag":item["organization"]["name"],
                               "Rörelse":"Ja" if "in_movement" in item.keys() and item["location"]["in_movement"] else "Nej",
                               "Modell": item["model"]["name"] if "model" in item.keys()  else None
                               })
        return gpd.GeoDataFrame(eqlist,geometry="geometry")
    
    def transform_coordinates(self,lon,lat):
        
        source_crs = pyproj.CRS('EPSG:4326')
        target_crs = pyproj.CRS('EPSG:3857')
        transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
        longitude, latitude = transformer.transform(lon, lat)
        return longitude,latitude

a=Abax()
a.create_organization_structure()
from time import sleep
import logging

# Configure logging to write to a file
logging.basicConfig(filename=os.path.join(os.path.dirname(__file__),'output.log'), level=logging.INFO)

# Redirect print statements to the logging module
def print(*args, **kwargs):
    logging.info(*args, **kwargs)
print("Hellå")