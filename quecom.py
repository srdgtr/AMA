from datetime import datetime
import logging
from pathlib import Path
import configparser
from time import sleep
import pandas as pd
import numpy as np
import requests
import sys
import os, glob
import dropbox

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file
from sqlalchemy import create_engine, MetaData, Table,update
from sqlalchemy.engine.url import URL

ini_config = configparser.ConfigParser()
ini_config.read(Path.home() / "bol_export_files.ini")
config_db = dict(
    drivername="mariadb",
    username=ini_config.get("database leveranciers", "user"),
    password=ini_config.get("database leveranciers", "password"),
    host=ini_config.get("database leveranciers", "host"),
    port=ini_config.get("database leveranciers", "port"),
    database=ini_config.get("database leveranciers", "database"),
)
odin_orders = dict(
    drivername="mariadb",
    username=ini_config.get("database odin", "user"),
    password=ini_config.get("database odin", "password"),
    host=ini_config.get("database odin", "host"),
    port=ini_config.get("database odin", "port"),
    database=ini_config.get("database odin", "database"),
)
engine = create_engine(URL.create(**config_db))
engine_odin = create_engine(URL.create(**odin_orders))
quecom_key = ini_config.get("quecom website", "api_key")
dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))
date = datetime.now().strftime("%c").replace(":", "-")
metadata = MetaData()

logger = logging.getLogger("ama_loging")
logging.basicConfig(
    filename="ama_" + datetime.now().strftime("%V") + ".log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)  # nieuwe log elke week
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

production_api, test_api = "v3", "mock"

if os.environ.get("PRODUCTION"):
    api = production_api
else:
    api = test_api

def check_limit(request):
    remaining_limit = int(request.headers.get("X-Rate-Limit-Remaining", 0))
    if remaining_limit < 1:
        print("no queries left")
        sleep(3600)


bol_orders = "SELECT I.orderid,I.order_orderitemid,I.dropship FROM orders_info_bol I WHERE I.created_on_artikel > DATE_ADD(NOW(), INTERVAL -1 MONTH) AND I.order_offerreference LIKE 'AMA%%' ORDER BY I.updated_on_artikel DESC"
blok_orders = "SELECT I.order_line_id,I.dropship FROM blokker_orders O LEFT JOIN blokker_order_items I ON O.commercialid = I.commercialid WHERE O.created_date > DATE_ADD(NOW(), INTERVAL -1 MONTH) AND I.offer_sku LIKE 'AMA%%' ORDER BY O.created_date DESC"
orders_bol = pd.read_sql(bol_orders,engine_odin).query("dropship < 3 or dropship != dropship")
orders_blok  = pd.read_sql(blok_orders,engine_odin).query("dropship < 3 or dropship != dropship")

def get_detail_order(apikey, version,order_ref):
    order = requests.get(f"https://quecom.eu/api/{version}/order/reference/{order_ref}", headers={"Authorization": f"Bearer {apikey}"})
    if order.status_code == 429:
        print("to many requests")
    if order.status_code == 200:
        return order.json()

def set_order_info_db_bol(order_info, track_en_trace_url, track_en_trace_num):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine_odin)
    logger.info(f"start stap 3 bol {order_info,track_en_trace_url, track_en_trace_num}")
    drop_send = (
        update(orders_info_bol)
        .where(orders_info_bol.columns.orderid == order_info)
        .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
    )
    with engine_odin.begin() as conn:
        conn.execute(drop_send)

def set_order_info_db_blokker(order_info, track_en_trace_url, track_en_trace_num):
    orders_info_blokker = Table("blokker_order_items", metadata, autoload_with=engine_odin)
    logger.info(f"start stap 3 blokker {order_info}")
    drop_send = (
        update(orders_info_blokker)
        .where(orders_info_blokker.columns.order_line_id == order_info)
        .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
    )
    with engine_odin.begin() as conn:
        conn.execute(drop_send)

for row in orders_bol.itertuples():
    order_info = get_detail_order(quecom_key, api,row.orderid)
    # print(order_info)
    if order_info and len(order_info.get("shipments")) > 0:
        reference = order_info.get("reference")
        track_en_trace_url = order_info.get("shipments")[0].get("tracking_url")
        track_en_trace_num = order_info.get("shipments")[0].get("tracking_code") 
        if track_en_trace_url and track_en_trace_num :
            set_order_info_db_bol(reference, track_en_trace_url, track_en_trace_num)

for row in orders_blok.itertuples():
    order_blok_info = get_detail_order(quecom_key, api,row.order_line_id)
    if order_blok_info and len(order_blok_info.get("shipments")) > 0:
        reference_blok = order_blok_info.get("reference") + "-1"
        track_en_trace_url_blok = order_blok_info.get("shipments")[0].get("tracking_url")
        track_en_trace_num_blok = order_blok_info.get("shipments")[0].get("tracking_code")
        if track_en_trace_url_blok and track_en_trace_num_blok :
            set_order_info_db_blokker(reference_blok, track_en_trace_url_blok, track_en_trace_num_blok)

def get_assortiment(apikey, version):
    assortiment = []
    headers = {"Authorization": f"Bearer {apikey}"}
    artikelen = requests.get(f"https://quecom.eu/api/{version}/assortment", headers=headers)
    if artikelen.status_code == 429:
        print("to many requests")
    if artikelen.status_code == 200:
        check_limit(artikelen)
        assortiment.extend(artikelen.json()["products"])
        while artikelen.json()["pagination"]["next_page"]:
            try:
                artikelen = requests.get(artikelen.json()["pagination"]["next_page"], headers=headers)
                if artikelen:
                    check_limit(artikelen)
                    assortiment.extend(artikelen.json()["products"])
                else:
                    break
            except KeyError:
                print("no_key")
                break
    return assortiment


if datetime.now().hour < 9 > 0:  # alleen in de nacht assortiment, want veranderd maar 1 keer per dag
    hele_assortiment = get_assortiment(quecom_key, api)
    if hele_assortiment:
        hele_assortiment_pd = (
            pd.DataFrame.from_dict(hele_assortiment)
            .assign(
                art_code=lambda x: x.description.str.get("short"),
                artikel_omschrijving=lambda x: x.description.str.get("full"),
                height_cm=lambda x: x.dimensions.str.get("height"),
                length_cm=lambda x: x.dimensions.str.get("length"),
                weight_kg=lambda x: x.weight.str.get("value"),
            )
            .drop(columns=["description"])
        )
        hele_assortiment_pd.to_csv(f"AMA_huidige_producten_{date}.csv", index=False)
    else:
        sys.exit()
else:
    hele_assortiment_pd = pd.read_csv(max(Path.cwd().glob("AMA_huidige_producten*.csv"), key=os.path.getmtime))


def get_current_stock(apikey, version):
    headers = {"Authorization": f"Bearer {apikey}"}
    stock_request = requests.get(f"https://quecom.eu/api/{version}/stock/all", headers=headers)
    if stock_request.status_code == 200:
        return stock_request.json()
    else:
        stock_request.text


def get_current_price(apikey, version):
    headers = {"Authorization": f"Bearer {apikey}"}
    stock_request = requests.get(f"https://quecom.eu/api/{version}/price/all", headers=headers)
    if stock_request.status_code == 200:
        return stock_request.json()
    else:
        stock_request.text


def get_current_product_groups(apikey, version):
    headers = {"Authorization": f"Bearer {apikey}"}
    stock_request = requests.get(f"https://quecom.eu/api/{version}/product-group", headers=headers)
    if stock_request.status_code == 200:
        return stock_request.json()
    else:
        stock_request.text


huidige_stock = get_current_stock(quecom_key, api)
huidige_price = get_current_price(quecom_key, api)
product_groups = get_current_product_groups(quecom_key, api)

huidige_stock_pd = pd.DataFrame.from_dict(huidige_stock).drop(columns="ean")
huidige_price_pd = pd.DataFrame.from_dict(huidige_price).drop(columns="ean")
product_grouping = pd.DataFrame.from_dict(product_groups).assign(
    categorie=lambda x: np.where(x.description.str.contains("^.{2} |^.{2}-"), x.description.str[3:], x.description)
)
huidige_assortiment_voorraad = (
    hele_assortiment_pd.merge(huidige_stock_pd, on="product_code")
    .merge(huidige_price_pd, on="product_code")
    .merge(product_grouping, on="product_group")
    .query("stock > 0")
    .rename(
        columns={
            "product_code": "Artikel",
            "ean": "EAN nummer",
            "title": "Art. omschrijving",
            "brand": "Merk",
            "srp": "SRP",
            "art_code": "Artikel Code Lev.",
            "categorie": "Categorie 3",
            "stock": "Beschikbaar",
            "price": "prijs",
        }
    )
)

huidige_assortiment_voorraad.to_csv("AMA_" + date + ".csv", index=False)

latest_file = max(glob.iglob("AMA_*.csv"), key=os.path.getctime)
with open(latest_file, "rb") as f:
    dbx.files_upload(
        f.read(),
        "/macro/datafiles/AMA/" + latest_file,
        mode=dropbox.files.WriteMode("overwrite", None),
        mute=True,
    )

# ama_info_db = ama_info[
#     [
#         "eigen_sku",
#         "sku",
#         "ean",
#         "voorraad",
#         "merk",
#         "prijs",
#         "advies_prijs",
#         "category",
#         "gewicht",
#         "url_plaatje",
#         "url_artikel",
#         "product_title",
#         "lange_omschrijving",
#         "verpakings_eenheid",
#         "lk",
#     ]
# ]

# huidige_datum = datetime.now().strftime("%d_%b_%Y")
# ama_info_db.to_sql(
#     f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000
# )

# engine.dispose()
