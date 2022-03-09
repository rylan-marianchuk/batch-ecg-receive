from datetime import datetime
import uuid
from utils import *
import xml.etree.ElementTree as ET
import sqlite3
import pandas as pd
import os
from sqlite_wrapper import SqliteDBWrap

class ReceiveECGs:

    def __init__(self, iter, URL, puid_map):
        """

        :param iter:
        :param URL:
        :param puid_map:
        """
        self.iter = iter
        self.CIROC_PATIENT_PATH = puid_map
        self.URL = URL

        self.subbatch_size = 3000
        self.rhrn_puid_map = get_rhrn2puid_mapping(self.CIROC_PATIENT_PATH)

        self.identified_attr = (pd.read_csv("idtags.txt", header=None)[0]).to_list()
        self.decoder_DB = SqliteDBWrap("decoder.db")
        cols = {
            "EUID": "TEXT PRIMARY KEY",
            "PUID": "TEXT",
            "BUID": "TEXT"
        }
        for col_name in self.identified_attr:
            cols[col_name] = "TEXT"
        self.decoder_DB.create_table("Decoder", cols)

        self.unparsable_DB = SqliteDBWrap("unparsable.db")
        self.unparsable_DB.create_table("Unparsable", {"FILENAME": "TEXT PRIMARY KEY", "BUID": "TEXT"})
        return



    def generate_summary(self, new_euids):
        """

        :param new_euids:
        :return:
        """
        return


    def receive_batch(self):
        buid = 'b' + str(uuid.uuid4())[:8]

        subbatch_progress = 0

        jsons = []
        extracted_ident = []
        unparsable = []

        for xml in os.listdir(self.iter):
            try:
                # Check for fundamentals: parses, RHRN (Patient ID), Rhythm Waveform, Acquisition Date and Time all exist
                tree = ET.parse(self.iter + "/" + xml)
                wvfm = tree.findall('.//Waveform')[1]
                date_time_reformat, date, time = getFormattedDateTime(tree)
                rhrn = tree.find('.//PatientID').text
                if rhrn is None: raise Exception()
            except:
                unparsable.append([xml, buid])
                continue

            if not rhrn in self.rhrn_puid_map:
                puid = 'p' + str(uuid.uuid4())[:8]
                self.rhrn_puid_map[rhrn] = puid
            else:
                puid = self.rhrn_puid_map[rhrn]

            # Duplication check
            if is_duplicate(puid, date, time, self.decoder_DB):
                continue

            # All checks have passed, now permitted to generate a new encounter
            euid = str(uuid.uuid4())

            # Split the tree to de-identify
            deid_tree, identified_elements = deidentify(tree, self.identified_attr)
            extracted_ident.append([euid, puid, buid] + identified_elements)

            # Embed the UID keys for future reference
            tree.find('.//PatientID').text = puid
            tree.find('.//PatientLastName').text = puid
            tree.find('.//DateofBirth').text = buid
            tree.find('.//PatientFirstName').text = euid

            # Send json to socket
            xml_string = ET.tostring(deid_tree.getroot()).decode()
            jsons.append(get_json_str(euid, puid, date_time_reformat, xml_string))

            subbatch_progress += 1
            if subbatch_progress == self.subbatch_size:
                self.decoder_DB.batch_write_listlists("Decoder", extracted_ident)
                send_json_to_socket(jsons, self.URL)
                self.unparsable_DB.batch_write_listlists("Unparsable", unparsable)

                # Empty containers
                jsons = []
                unparsable = []
                extracted_ident = []
                subbatch_progress = 0

        self.decoder_DB.batch_write_listlists("Decoder", extracted_ident)
        self.unparsable_DB.batch_write_listlists("Unparsable", unparsable)
        self.decoder_DB.exit()
        self.unparsable_DB.exit()

        send_json_to_socket(jsons, self.URL)

        write_rhrn2puid_mapping(self.rhrn_puid_map, self.CIROC_PATIENT_PATH)
        self.generate_summary([])
        return