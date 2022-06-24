# -*- coding: utf-8 -*-
# spider for quecom/ amacom
# zou ook vaker kunnen worden uitgevoerd

# scrapy crawl quecom.nl

import configparser
import glob
import os
import sys
from datetime import datetime
from pathlib import Path

import dropbox
import pandas as pd
from scrapy import Spider
from scrapy.http import FormRequest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

date = datetime.now().strftime("%c").replace(":", "-")


class QuecomNlSpider(Spider):
    name = "quecom.nl"
    allowed_domains = ["quecom.nl"]
    start_urls = ["https://client.quecom.nl/auth"]

    def parse(self, response):
        return FormRequest.from_response(
            response,
            formdata={
                "password": alg_config.get("quecom website", "password"),
                "email": alg_config.get("quecom website", "email"),
            },
            callback=self.scrape_pages,
        )

    def scrape_pages(self, response):
        csv_file = response.urljoin(response.xpath("//a[contains(text(),'Beide')]/@href").extract_first())
        if csv_file:
            yield response.follow(csv_file, self.save_file)

    def save_file(self, response):
        with open("amacom_temp_" + date + ".csv", "wb") as f:  # open a new file
            f.write(response.body)  # write content downloaded

    def close(self, reason):
        quecom_final = (
            pd.read_csv(max(glob.iglob("amacom_temp_*.csv"), key=os.path.getctime), sep=";")
            .query("Beschikbaar > 0")
            .query("`EAN nummer` == `EAN nummer`")
            .assign(
                prijs=lambda x: pd.to_numeric(x["Netto inkoopprijs"], errors="coerce"),
                lk=lambda x: (korting_percent * x["prijs"] / 100).round(2),
            )
            .assign(prijs=lambda x: x["prijs"] - x["lk"].round(2))
            .drop(columns="Netto inkoopprijs")
        )
        quecom_final.to_csv("AMA_" + date + ".csv", index=False)
        ama_info = quecom_final.assign(
            eigen_sku=lambda x: "AMA" + x["Artikel"].astype(str),
            advies_prijs="",
            gewicht="",
            url_plaatje="",
            url_artikel="",
            lange_omschrijving="",
            verpakings_eenheid="",
        ).rename(
            columns={
                "Artikel": "sku",
                "EAN nummer": "ean",
                "Beschikbaar": "voorraad",
                "Merk": "merk",
                "Categorie 1": "category",
                "Art. omschrijving": "product_title",
                "stock": "voorraad",
            }
        )
        latest_file = max(glob.iglob("AMA_*.csv"), key=os.path.getctime)
        with open(latest_file, "rb") as f:
            dbx.files_upload(
                f.read(),
                "/macro/datafiles/AMA/" + latest_file,
                mode=dropbox.files.WriteMode("overwrite", None),
                mute=True,
            )
        ama_info_db = ama_info[
            [
                "eigen_sku",
                "sku",
                "ean",
                "voorraad",
                "merk",
                "prijs",
                "advies_prijs",
                "category",
                "gewicht",
                "url_plaatje",
                "url_artikel",
                "product_title",
                "lange_omschrijving",
                "verpakings_eenheid",
                "lk",
            ]
        ]

        huidige_datum = datetime.now().strftime("%d_%b_%Y")
        ama_info_db.to_sql(
            f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000
        )

        with engine.connect() as con:
            con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
            aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            totaal_stock = int(
                con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            )
            totaal_prijs = int(
                con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            )
            leverancier = f"{current_folder}"
            sql_insert = "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
            con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

        engine.dispose()
