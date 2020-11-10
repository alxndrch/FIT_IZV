# file: download.py
# description: IZV 1. project
# data: 1.11 2020
# author: Alexandr Chalupnik
# email: <xchalu15@stud.fit.vutbr.cz>

"""
    Soubor stahne a zpracuje data o nehodovosti, 
    pokud nebude program spousten jako modul,
    zpracuje data pro Prahu, Jihomoravsky a Zlinsky kraj, 
    k tomu se vypisou se hlavicka csv souboru a pocet zpracovanych radku
    
    Priklad spusteni:
    python download --show_figure
    
    parametry pri spusteni
    --show_figure
        zobrazi graf poctu nehod pro dosputne roky, v krajích PHA, JHM, ZLK
    --fig_location
        cilova slozka pro ulozeni grafu
        
    Tabulka Kraju
        PHA: "00", 
        CTS: "01",
        JHC: "02",
        PLK: "03", 
        KVK: "19",
        ULK: "04",
        LBK: "18",
        HKK: "05", 
        PAK: "17",
        OLK: "14",
        MSK: "07",
        JHM: "06", 
        ZLK: "15",
        VYS: "16"
        
    CSV hlavicka
    "identifikační číslo", "druh pozemní komunikace", "číslo pozemní komunikace", 
    "den, měsíc, rok", "týden", "čas", "druh nehody", "druh srážky jedoucích vozidel",
    "druh pevné překážky", "charakter nehody", "zavinění nehody", "alkohol u viníka nehody přítomen",
    "hlavní příčiny nehody", "usmrceno osob", "těžce zraněno osob", "lehce zraněno osob", 
    "celková hmotná škoda", "druh povrchu vozovky", "stav povrchu vozovky v době nehody", 
    "stav komunikace", "povětrnostní podmínky v době nehody", "viditelnost", "rozhledové poměry",
    "dělení komunikace", "situování nehody na komunikaci", "řízení provozu v době nehody",
    "místní úprava přednosti v jízdě", "specifická místa a objekty v místě nehody", 
    "směrové poměry", "počet zúčastněných vozidel", "místo dopravní nehody", 
    "druh křižující komunikace", "druh vozidla", "výrobní značka motorového vozidla", "rok výroby vozidla", 
    "charakteristika vozidla", "smyk", "vozidlo po nehodě", "únik provozních, přepravovaných hmot",
    "způsob vyproštění osob z vozidla", "směr jízdy nebo postavení vozidla", "škoda na vozidle",
    "kategorie řidiče",  "stav řidiče", "vnější ovlivnění řidiče", "a", "b", "d", "e", "f", "g",
    "h", "i", "j", "k", "l", "n", "o", "p", "q", "r", "s", "t", "lokalita nehody", "kraj"
"""

import zipfile
from bs4 import BeautifulSoup
from io import TextIOWrapper
from zipfile import BadZipFile, ZipFile

import argparse
import csv
import gzip
import numpy as np
import os
import pickle
import requests
import re
from requests.exceptions import RequestException

from requests.models import MissingSchema

from get_stat import plot_stat

class DataDownloader():
    """
    Agregator statistik nehodovnosti Policie CR
    
    Attributes
    ----------
    REGION_CODES : dict
        kod regionu : nazev csv souboru
    DATA_TYPES : list
        datove typy pro kazdy sloupec csv 
    CSV_HEADER : list
        popis hlavicky csv
    Methods
    -------
    donwload_data()
        stahne data z url 
    parse_region_data(region)
        naparsuje data z csv souboru pro dany region
    get_list(regions=None)
        vraci data za vsechny dostupne roky pro zadane regiony
        implicitne pro vsechny regiony
    find_zips(html)
        najde v html kodu cestu k zip souborum
    """
    
    REGION_CODES = { "PHA": "00", "CTS": "01", "JHC": "02", "PLK": "03", 
                    "KVK": "19", "ULK": "04","LBK": "18", "HKK": "05", 
                    "PAK": "17", "OLK": "14", "MSK": "07", "JHM": "06", 
                    "ZLK": "15", "VYS": "16" }

    DATA_TYPES = [ "U12", "i8", "U8", "datetime64[D]", "i8", "i8", "i8", "i8", 
               "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8",
               "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", 
               "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", "i8", 
               "i8", "i8", "i8", "i8", "i8", "i8", "i8", "f8", "f8", "f8", 
               "f8", "f8", "f8", "<U64", "<U64", "<U64", "<U64", "<U64", 
               "<U64", "<U64", "<U64", "<U64", "i8", "i8", "<U64", "i8", "U3" 
               ]

    CSV_HEADER = [ "p1", "p36", "p37", "p2a", "weekday(p2a)", "p2b", "p6", "p7", 
               "p8", "p9", "p10", "p11", "p12", "p13a", "p13b", "p13c", "p14",
               "p15", "p16", "p17", "p18", "p19", "p20", "p21", "p22", "p23", 
               "p24", "p27", "p28", "p34", "p35", "p39", "p44", "p45a", "p47",
               "p48a", "p49", "p50a", "p50b", "p51", "p52", "p53", "p55a", 
               "p57", "p58", "a", "b", "d", "e", "f", "g", "h", "i", "j", "k",
               "l", "n", "o", "p", "q", "r", "s", "t", "p5a", "kraj" ]

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", 
                 cache_filename="data{}.pkl.gz"):
        """
        Parameters
        ----------
        url : str
            url adresa s dostupnymi daty
            implicitne "https://ehw.fit.vutbr.cz/izv/"
        folder : str
            slozka do ktere se data ulozi
            implicitne "data"
        cache_filename : str
            jmeno souboru, pro zpracovane data z funkce get_list,
            mezi {} se doplni kod zpracovavaneho regionu
            implicitne "data{}.pkl.gz
        years: dict 
            rok : posledni aktualni soubor ke stazeni
        pickled_data : 
            nactena data z cache_filename
        """
        self.url = url
        self.folder = folder
        self.cache_filename = cache_filename
        self.years = {}
        self.pickled_data = None

    def download_data(self):
        """Funkce stáhne do složky ​folder​ všechny soubory s daty z adresy ​url"""
        
        try:
            if not os.path.exists(self.folder):
                os.makedirs(self.folder)
        except OSError as err:
            print(f"Chyba pri vytvareni souboru: {err}")
            exit(1)
            
        UA = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"
        headers = {"User-Agent": UA}
        sess = requests.Session()
        
        try:
            resp = sess.get(self.url, headers=headers)
        except RequestException:
            print(f"Neplatna url: {self.url}")
            exit(1)

        self.find_zips(resp.text)
        
        if not self.years.values():
            print("Nenalezena nebyly nalezeny zadne soubory")
            exit(1)

        for path in self.years.values():  # stazeni vsech souboru 
            zip_file = os.path.basename(path)
            if os.path.exists(os.path.join(self.folder, zip_file)):
                continue  # pokud soubor existuje v adresari path, nestahuje se

            resp = sess.get(os.path.join(self.url, path), stream=True)
            with open(os.path.join(self.folder, zip_file), "wb") as fd:
                for chunk in resp.iter_content(chunk_size=128):
                    fd.write(chunk)

    def parse_region_data(self, region):
        """
        naparsuje data z csv souboru pro dany region a vrati 
        
        Parameters
        ----------
        region : str
            trimistny kod rerionu
            
        Returns
        -------
        tuple(list(str), list(np.ndarray))
            prvni polozka: seznam retezcu, odpovídá názvům jednotlivých datových sloupců
            druha polozka: seznma NumPy poli, obsahuje data pro region
        """
        
        if region not in self.REGION_CODES.keys():
            raise InvalidRegion(region, "invalid region code")
        
        if not self.years:
            self.download_data()
        
        filtered_data = []
        try:
            for year in self.years.values():
                with ZipFile(os.path.join(self.folder, os.path.basename(year))) as zipf:
                    with zipf.open(f"{self.REGION_CODES[region]}.csv","r") as csvf:
                        csv_rows = csv.reader(TextIOWrapper(csvf, encoding="windows-1250"), delimiter=";")                    

                        for i, row in enumerate(csv_rows):
                            for j, col in enumerate(row):
                                if self.DATA_TYPES[j].startswith(("i", "f", "d")):
                                    if col == "" or re.search("[a-zA-Z]", col):
                                        row[j] = "-1"
                                        continue
                                    row[j] = col.replace(",", ".")
                                
                                if j == 34 and col == "XX": row[j] = "-1"

                            row.append(region)
                            filtered_data.append(row)
        except:
            print("Chyba pri otevirani zip souboru")
            exit(1)

        data = np.array(filtered_data).T
        data = [*data]
        for i, d in enumerate(data):
            data[i] = d.astype(self.DATA_TYPES[i])

        return (self.CSV_HEADER, data)

    def get_list(self, regions=None):
        """
        Vrací zpracovaná data pro vybrané kraje (regiony).
        
        Parameters
        ----------
        regions : list
            seznam regionu (trimistny kod)
            implicitne None, tzn. vraci data pro vsechny regiony
            
        Raises
        ------
        InvalidRegion
            pokud neni parametr regions list, nebo je v seznamu alespon jeden neplatny region
            
        Returns
        -------
        tuple(list(str), list(np.ndarray))
            prvni polozka: seznam retezcu, odpovídá názvům jednotlivých datových sloupců
            druha polozka: seznma NumPy poli, obsahuje data pro region
        """
        
        if regions is None:
            regions = self.REGION_CODES.keys()
        else:
            if not isinstance(regions, list):
                print("Neplatně zadane regiony")
                exit(1)
            if not all(item in self.REGION_CODES.keys() for item in regions):
                print("Neplatny kod kraje v seznamu")
                exit(1)
        buff = None
        for region in regions:
            pickle_path = os.path.join(self.folder, self.cache_filename.format(region))
            if self.pickled_data is None:
                if os.path.exists(pickle_path):
                    with gzip.open(pickle_path, "rb") as pklf:
                        self.pickled_data = pickle.load(pklf)
                else:
                    data = self.parse_region_data(region)
                    with gzip.open(pickle_path, "wb") as gzipf:
                        pickle.dump(data, gzipf)
                        self.pickled_data = data
    
            if buff is None:
                buff = np.array(self.pickled_data[1])
            else:
                buff = np.concatenate((buff, np.array(self.pickled_data[1])), axis=1)

            self.pickled_data = None

        return (self.CSV_HEADER, [*buff])

    def find_zips(self, html):
        """
        Najde v html kodu cestu k zip souborum
        
        Parameters
        ----------
        html : list
            html stranka     
        """
        
        year = None
        soup = BeautifulSoup(html, "html.parser")
        try:
            for tr in soup.find_all("tr"):  # ulozeni cesty k poslednimu zipu kazdeho roku
                if m := re.fullmatch(r"20..", tr.contents[0].string):
                    self.years[m[0]], year = None, m[0]

                for a in tr.find_all("a"):
                    if m := re.fullmatch(r".*\.zip", a["href"]):
                        self.years[year] = m[0]
        except:
            print("Chyba pri parsovani webove stranky")
            exit(1)

        self.years =  {k: v for k, v in self.years.items() if v is not None }
    

def process_args():
    """zpracovani argumentu z prikazove radky"""
    
    parser = argparse.ArgumentParser(description="Statistiky nehodovosti Policie CR", allow_abbrev=False)
    parser.add_argument("--show_figure", help="if set show figures", action="store_true")
    parser.add_argument("--fig_location", help="folder for figures")
    return parser.parse_args()

if __name__ == "__main__":
    DEF_REGIONS = ["ZLK", "PHA", "JHM"]    
    args = process_args()
    data_source = DataDownloader().get_list()
    plot_stat(data_source, args.fig_location, args.show_figure)

    print(f"columns: {', '.join(data_source[0])}\nRegions: {', '.join(DEF_REGIONS)}\nNumber of records: {len(data_source[1][0])}")