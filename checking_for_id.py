import xml.etree.ElementTree as ET
import os

tags2replace = {
    "PatientLastName": None,
    "PatientFirstName": None,
    "LocationName": ' ',
    "DateofBirth": ' ',
    "Gender": None,
    "PatientAge": None
}

for elem in ET.parse('./May_2019_XML/MUSE_20190315_120232_20000.xml').iter():
    if elem.tag in tags2replace: continue
    if elem.tag == "QRSTimesTypes": break
    tag_string = elem.tag.lower()
    if "date" in tag_string or "time" in tag_string or "name" in tag_string or "location" in tag_string:
        tags2replace[elem.tag] = ' '

for key in tags2replace.keys():
    print(key)

print("Checking:")

for xml in os.listdir("./May_2019_XML"):
    for elem in ET.parse("./May_2019_XML/"  + xml).iter():
        if elem.tag in tags2replace: continue
        tag_string = elem.tag.lower()
        if "date" in tag_string or "time" in tag_string or "name" in tag_string or "location" in tag_string:
            print(tag_string + "\t\t!!")

