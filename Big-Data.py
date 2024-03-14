
# wczytanie odpowiednich bibliotek
import pandas as pd
from unidecode import unidecode
import requests
from bs4 import BeautifulSoup

# zmiana wyswietlania na pelna ilosc kolumn, aby widziec nad czym pracujemy
pd.set_option("display.max_columns", None)

# wczytanie zbioru i odpowiednie zmiany nazw kolumn
brvehins = pd.read_csv("arq_casco_comp.csv", sep=";")
brvehins = brvehins.rename(columns={
    "SEXO": "Gender",
    "IDADE": "age_code",
    "ANO_MODELO": "VehYear_production",
    "IS_MEDIA": "SumInsAvg",
    "FREQ_SIN1": "ClaimNbRob",
    "FREQ_SIN2": "ClaimNbPartColl",
    "FREQ_SIN3": "ClaimNbTotColl",
    "FREQ_SIN4": "ClaimNbFire",
    "FREQ_SIN9": "ClaimNbOther",
    "INDENIZ1":  "ClaimAmountRob",
    "INDENIZ2": "ClaimAmountPartColl",
    "INDENIZ3": "ClaimAmountTotColl",
    "INDENIZ4": "ClaimAmountFire",
    "INDENIZ9": "ClaimAmountOther",
    "PREMIO1": "PremTotal",
    "REGIAO": "State_code",
    "EXPOSICAO1": "ExposTotal",
    "EXPOSICAO2": "ExposFireRob",
    "PREMIO2": "PremFireRob"
})

# tworzymy kolumne z wiekiem auta, a nie rokiem produkcji
brvehins["VehYear"] = 2024 - brvehins["VehYear_production"]
brvehins.drop("VehYear_production", axis=1, inplace=True)

# dodanie nazwy samochodu i ich grupy do naszej tabeli
left = pd.read_csv("auto2_vei1.csv", sep=";")
brvehins = brvehins.merge(left, left_on="COD_MODELO", right_on="CODIGO", how="left" )
brvehins.drop("CODIGO", axis=1, inplace=True)
brvehins.drop("COD_GRUPO", axis=1, inplace=True)
brvehins.drop("ENVIO", axis=1, inplace=True)
brvehins = brvehins.rename(columns={
    "DESCRICAO": "FullVehCode",
    "GRUPO": "VehCode"
})

# dodanie kolumny z odpowiednim labelem na kolumnie z kodem grupy wiekowej
left = pd.read_csv("auto_idade.csv" , sep=";")
brvehins = brvehins.merge(left, left_on="age_code", right_on="codigo", how="left")
brvehins.drop(["codigo", "age_code", "COD_MODELO", "COD_TARIF"], axis=1, inplace=True)
brvehins = brvehins.rename(columns={"descricao": "DrivAge"})

# konwetrowanie kolumn na odpowiednie typy
brvehins["SumInsAvg"] = brvehins["SumInsAvg"].str.replace(",", ".").astype(float)
brvehins["ExposTotal"] = brvehins["ExposTotal"].str.replace(",", ".").astype(float)
brvehins["PremTotal"] = brvehins["PremTotal"].str.replace(",", ".").astype(float)

# dropowanie wierszy z nieokreslonym stanem
wh = brvehins["State_code"] == " ."
brvehins = brvehins[~wh]

# left join na kolumnie z kodem stanu i z odpowiednia csv i odpowiednie konwertowanier na typ danych
left = pd.read_csv("auto_reg.csv", sep=";")
brvehins["State_code"] = brvehins["State_code"].astype(float)
brvehins = brvehins.merge(left, left_on="State_code", right_on="CODIGO", how="left")
brvehins.drop("State_code", axis=1, inplace=True)
brvehins = brvehins.rename(columns={"DESCRICAO": "state_split"})

# podzielenie kolumny state_split na dwie osobne z kodem stanu i nazwa Area
brvehins[["StateAB", "Area"]] = brvehins["state_split"].str.split(" - ", expand=True)
brvehins.drop(["state_split", "CODIGO"], axis=1, inplace=True)

# zczytanie tabeli z nazwami stanow z wikipedii
wikiurl = "https://en.wikipedia.org/wiki/Federative_units_of_Brazil"
response = requests.get(wikiurl)
soup = BeautifulSoup(response.text, "html.parser")
indiatable = soup.find("table", {"class": "wikitable"})
state_full = pd.read_html(str(indiatable))
state_full = pd.DataFrame(state_full[0])

# dodanie kolumny State za pomoca joina
state_left = state_full[["Code", "Flag and name"]]
brvehins = brvehins.merge(state_left, left_on="StateAB", right_on="Code", how="left")
brvehins.drop("Code", axis=1, inplace=True)
brvehins = brvehins.rename(columns={"Flag and name": "State"})

#left = pd.read_csv("Zeszyt1.csv", sep=";")
#brvehins = brvehins.merge(left, left_on="StateAB", right_on="code", how="left")
#brvehins = brvehins.rename(columns={"name": "State"})
#brvehins.drop("code", axis=1, inplace=True)
# Zastapilismy ta czesc kodu na czytanie informacji z tabeli z wikipedi. Wczesniej zrobilismy to za pomoca dodatkowego pliku csv pobranego z innej strony.

# petla zmieniajace znaki specjalne na litery z alfabetu angielskeigo
for col in brvehins.columns:
    if brvehins[col].dtype == "object":
        brvehins[col] = brvehins[col].apply(lambda x: unidecode(str(x)) if pd.notnull(x) else x)
    else:
        continue

# zmiana kolejnosci kolumn w ramce danych
order = ["Gender", "DrivAge", "VehYear", "FullVehCode", "VehCode",
         "Area", "State", "StateAB", "ExposTotal", "ExposFireRob",
         "PremTotal", "PremFireRob", "SumInsAvg",
         "ClaimNbRob", "ClaimNbPartColl", "ClaimNbTotColl", "ClaimNbFire", "ClaimNbOther",
         "ClaimAmountRob", "ClaimAmountPartColl", "ClaimAmountTotColl", "ClaimAmountFire", "ClaimAmountOther"]

brvehins = brvehins[order]

# glowa tabeli i informacja o typach kolumn zgodnych z oryginalnym plikiem
print(brvehins.head())
print(brvehins.info())

# autorzy
# Anna Hutnik
# Dawid Bonar
# Kacper Gonet
# Maciej Laburda