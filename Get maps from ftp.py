from ftplib import FTP
import io, os
import json
import zipfile
import fiona,geopandas
from pyproj import CRS
from datetime import date,timedelta
import datetime
import pandas as pd
from functools import wraps
import time
import tempfile
import shutil
def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {execution_time:.6f} seconds.")
        return result
    return wrapper

class Kartor:
    def __init__(self):
        host = 'ftp.tkgbg.se'
        username = 'Samordent'
        password = 'Vinter2020'
        self.ftp = FTP(host)
        self.ftp.login(username, password)
        self.ftp.encoding='latin-1'
        
        self.zipmap = os.path.join(os.path.dirname(__file__),'zip')
        self.shapemap = os.path.join(os.path.dirname(__file__),'shp')
        if not os.path.exists(self.shapemap): os.makedirs(self.shapemap)
        if not os.path.exists(self.zipmap): os.makedirs(self.zipmap)

    def print_file_info(self):
        file_list = []
        self.ftp.retrlines('LIST', file_list.append) 
        for item in file_list:print(item)
    def download_file(self,filename='Gräs_Slåtter_Natur_SamEnt.zip'):
        file = io.BytesIO()
        self.ftp.retrbinary('RETR ' + filename, file.write)
        file.seek(0)
        return file

    def zip_files(self):
        file = self.download_file()
        zip_file = zipfile.ZipFile(file)
        namelist = zip_file.namelist()
        for item in namelist:
            if not item.endswith('.tab'): continue
            file_content = zip_file.read(item)
            bytesio = io.BytesIO(file_content)
            bytesio.seek(0)
            
            break
        zip_file.extractall(self.zipmap)
        return "Extracted all files in zip"

    def create_shapefiles(self):
        for filename in os.listdir(self.zipmap):
            if not filename.endswith('.tab'):continue
            filepath = os.path.join(self.zipmap,filename)

            with fiona.open(filepath) as src:
                gdf = geopandas.GeoDataFrame.from_features(src)
            gdf = geopandas.GeoDataFrame.from_file(filepath)
            if not "anlaggning" in gdf.columns:
                if not "anlaggningsnamn" in gdf.columns: 
                    print(gdf.columns)
                    continue
                gdf["anlaggning"] = gdf.pop("anlaggningsnamn")
            gdf = gdf#[gdf["kund"]].str.contains("Stena",case=False)]
            if gdf.empty:continue
            gdf = self.reproject_layer(gdf)
            #gdf.to_file(os.path.join(os.path.join(os.path.dirname(__file__),'Excel'),filename[:-4]+'.shp'))
            gdf.to_file(os.path.join(self.shapemap,filename[:-4]+'.shp'),geometry="geometry")

        self.ftp.quit()
        return gdf

    def reproject_layer(self, layer, crs = 3857):

        target_crs = CRS.from_epsg(crs)
        reprojected_layer = layer.to_crs(target_crs)
        return reprojected_layer
    

def return_all_ids(kartor):
    def find_shp_files(directory):
        shp_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.shp'):
                    shp_files.append(os.path.join(root, file))
        return shp_files
    shp_files = find_shp_files(kartor)
    ids = []
    for shp in shp_files:
        gdf = geopandas.GeoDataFrame.from_file(shp)
        if "Arealskiss" in gdf.columns: gdf["skiss_yta"] = gdf.pop("Arealskiss")
        if "Arealskiss" not in gdf.columns and "skiss_yta" not in gdf.columns:
            continue
        ids=ids+list(gdf["skiss_yta"])
    return ids

def get_updates(filepath, kartor,delta,full_run=True):
    if full_run:

        ids = return_all_ids(kartor)
        with open("ids.txt",'w') as f:
            json.dump(ids,f,indent=3,ensure_ascii=False)
    else:
        with open("ids.txt",'r', encoding="utf-8") as f:
            ids = json.load(f)
    now = date.today()
    print("Hello",full_run)
    time_before = now + timedelta(days=delta)
 
    gg = geopandas.GeoDataFrame()
    for root, dirs, files in os.walk(filepath):
        for file in files:
            fpath = os.path.join(root,file)
            if fpath.endswith('.tab') or fpath.endswith('.shp'):
                gdf = check_file_against_ids(fpath, ids,time_before)
                if not gdf.empty:
                    
                    try:
                        gg = pd.concat([gg,gdf])
                    except:
                        crs = 3857
                        try: gg=pd.concat([gg.to_crs(CRS.from_epsg(crs)),gdf.to_crs(CRS.from_epsg(crs))])
                        except Exception as e:
                            print(str(e))
    return gg

def check_file_against_ids(gdf, ids,time_before):
    if type(gdf) == str:gdf = geopandas.GeoDataFrame.from_file(gdf)
    if any([key in gdf.columns for key in ["Arealskiss","Ytnummer","IDnr"]]): gdf["skiss_yta"] = gdf.pop("Ytnummer")
    if "ID_1" in gdf.columns:gdf["skiss_yta"] = gdf.pop("ID_1")
    elif "id" in gdf.columns:gdf["skiss_yta"] = gdf.pop("id")
    elif "Feature_id" in gdf.columns:gdf["skiss_yta"] = gdf.pop("Feature_id")
    elif "TO_ID" in gdf.columns:gdf["skiss_yta"] = gdf.pop("TO_ID")

    gdf = gdf[gdf["skiss_yta"].isin(ids)]
    gdf["skiss_yta"] = gdf["skiss_yta"].dropna()
    if "Uppdaterad" in gdf.columns: gdf["uppdaterad"] = gdf.pop("Uppdaterad")
    if "uppdaterad" not in gdf.columns: return gdf
    gdf["uppdaterad"] = gdf["uppdaterad"].map(lambda x: transform_time(x))
    #print(gdf["uppdaterad"])
    gdf = gdf[gdf["uppdaterad"]>time_before]
    gdf["uppdaterad"] = gdf["uppdaterad"].map(lambda x: datetime.datetime.strftime(x,"%Y-%m-%dZ") if x else None)
    if not gdf.empty:print(gdf)
    return gdf
    
def transform_time(datestring):
    if type(datestring)==float:return None
    if not datestring:return None
    converted_date=None
    try:converted_date=datetime.datetime.strptime(datestring,"%Y-%m-%d").date()
    except Exception as e: 
        try:converted_date=datetime.datetime.strptime(datestring,"%Y-%m-%dZ").date()
        except Exception as e:
            print("------------------------------------")
            print(str(e))
            print("------------------------------------")
        return converted_date
            
@timeit
def run_functions(filepath,kartor,delta,full_run=True):
    #kartor = os.path.join(os.path.dirname(__file__),'Kartor')
    if full_run:
        try:
            ftp = Kartor()
            ftp.zip_files()
            ftp.create_shapefiles()
            kartor = os.path.join(os.path.dirname(__file__),'Kartor')
            if not os.path.exists(kartor):os.makedirs(kartor)
            
            df = get_updates(filepath,kartor,delta,full_run=True)
        except Exception as e:
            return {"status":500, "status":str(e)}
    else: df = get_updates(filepath, kartor,delta, full_run=False)
    if df.empty:
        return {"status":200, "status":"No new updates."}
    df.to_file('updates.json')
    df = geopandas.read_file("updates.json")
    jsonfile = df.to_json()
    return {"status":200, "status":"Updates in json-file!","json":jsonfile}
    
def check_last_update(fpath):
        timestamp = os.path.getmtime(fpath)
        date_last_update = datetime.datetime.fromtimestamp(timestamp).date()
        return date_last_update
if __name__ == '__main__':
    
    shapefiles_path = os.path.join(os.path.dirname(__file__),'zip')
    used_maps_path = os.path.join(os.path.dirname(__file__),'Kartor')
    days_back = -30
    kartor = Kartor()
    zip = kartor.download_file()
    def reproject_layer(layer, crs = 3857):

        target_crs = CRS.from_epsg(crs)
        reprojected_layer = layer.to_crs(target_crs)
        return reprojected_layer
    @timeit
    def open_project_in_zipfile(zipp,delta):
        temp_dir = tempfile.mkdtemp()
        try:
            zip_file = zipfile.ZipFile(zipp)
            zip_file.extractall(temp_dir)
            gdg = geopandas.GeoDataFrame()
            ids = return_all_ids(os.path.join(os.path.dirname(__file__),'Kartor'))
            for item in zip_file.namelist():
                if not item.endswith('.tab') or item.endswith('.shp'):continue
                fpath = os.path.join(temp_dir,item)
                gdf = geopandas.read_file(fpath)
                gdf = reproject_layer(gdf)
                time_before = datetime.datetime.now() + datetime.timedelta(days=delta)
                gdf = check_file_against_ids(gdf,ids,time_before.date())
                if not gdf.empty:
                    gdg = pd.concat(gdg,gdf)
            return gdg

                
        finally: shutil.rmtree(temp_dir)    
        
    delta = -60 # 60 dagar tillbaka.
    
    gdf = open_project_in_zipfile(zip,delta)
    print(gdf.head())