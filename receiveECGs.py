from datetime import datetime
import uuid
from utils import *
import xml.etree.ElementTree as ET
import sqlite3
import pandas as pd

class ReceiveECGs:

    def __init__(self, URL, puid_map):
        """

        :param URL:
        :param puid_map:
        """

        self.CIROC_PATIENT_PATH = puid_map
        self.URL = URL
        self.rhrn_puid_map = get_rhrn2puid_mapping(self.CIROC_PATIENT_PATH)
        self.subbatch_size = 3000

        self.decoder_conx = sqlite3.connect("decoder.db")
        self.cursor = self.decoder_conx.cursor()

        self.identified_attr = (pd.read_csv("idtags.txt", header=None)[0]).to_list()

        self.cursor.execute("CREATE TABLE IF NOT EXISTS Decoder("
                            "EUID TEXT PRIMARY KEY, "
                            "PUID TEXT,"
                            + " TEXT, ".join(self.identified_attr) +
                            ");")

        self.decoder_conx.commit()
        self.identified_attr = (pd.read_csv("idtags.txt", header=None)[0]).to_list()
        return



    def receive_batch(self, iterable):

        buid = str(uuid.uuid4())

        subbatch_progress = 0

        jsons = []
        extracted_ident = []

        for xml in iterable:
            try:
                # Check for fundamentals: parses, RHRN (Patient ID), Rhythm Waveform, Acquisition Date and Time all exist
                tree = ET.parse('./May_2019_XML/' + xml)
                wvfm = tree.findall('.//Waveform')[1]
                date_time = getFormattedDateTime(tree)
                rhrn = tree.find('.//PatientID').text
                if rhrn is None: raise Exception()
            except:
                print(xml + str(" was not able to be parsed, moving on"))
                continue

            if not rhrn in self.rhrn_puid_map:
                puid = str(uuid.uuid4())
                self.rhrn_puid_map[rhrn] = puid
            else:
                puid = self.rhrn_puid_map[rhrn]

                # Duplication check
                #if date_time in get_acq_datetimes(puid, decoder_conx):
                #    continue

            euid = str(uuid.uuid4())

            deid_tree, identified_elements = deidentify(tree, self.identified_attr)

            tree.find('.//PatientID').text = puid
            tree.find('.//PatientLastName').text = puid
            tree.find('.//DateofBirth').text = buid
            tree.find('.//PatientFirstName').text = euid

            extracted_ident.append([euid, puid] + identified_elements)

            xml_string = ET.tostring(deid_tree.getroot()).decode()
            jsons.append(get_json_str(euid, puid, date_time, xml_string))


            subbatch_progress += 1
            if subbatch_progress == self.subbatch_size:
                write_ident_to_sql(extracted_ident, self.decoder_conx)
                send_json_to_socket(jsons, self.URL)
                # Empty containers
                jsons = []
                extracted_ident = []
                subbatch_progress = 0

        write_ident_to_sql(extracted_ident, self.decoder_conx)
        send_json_to_socket(jsons, self.URL)

        write_rhrn2puid_mapping(self.rhrn_puid_map, self.CIROC_PATIENT_PATH)
        self.generate_summary([])
        return


    def generate_summary(self, new_euids):
        """

        :param new_euids:
        :return:
        """
        return


    def close_decoder_db(self):
        self.decoder_conx.close()
        return