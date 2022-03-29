import uuid
import time as prgm_time
from src.utils import *
import xml.etree.ElementTree as ET
import pandas as pd
import os
from src.sqlite_wrapper import SqliteDBWrap

class ReceiveECGs:

    def __init__(self, xml_dir, URL, puid_map, h5_dir):
        """

        :param iter:
        :param URL:
        :param puid_map:
        """
        self.xml_dir = xml_dir
        self.CIROC_PATIENT_PATH = puid_map
        self.URL = URL
        self.h5_dir = h5_dir

        self.subbatch_size = 3000
        self.rhrn_puid_map = get_rhrn2puid_mapping(self.CIROC_PATIENT_PATH)

        self.identified_attr = (pd.read_csv("./src/idtags.txt", header=None)[0]).to_list()
        self.decoder_DB = SqliteDBWrap("./database/decoder.db")
        decoder_cols = {
            "EUID": "TEXT PRIMARY KEY",
            "PUID": "TEXT",
            "BUID": "TEXT"
        }
        for col_name in self.identified_attr:
            decoder_cols[col_name] = "TEXT"
        self.decoder_DB.create_table("Decoder", decoder_cols)

        self.waveformfeatures_DB = SqliteDBWrap("./database/waveformfeatures.db")
        wvfm_features_cols = {
            "EUID_LEAD": "TEXT PRIMARY KEY",
            "BUID": "TEXT",
            "LEAD": "INT",
            "NOCHANGE20": "INT",
            "CURVELENGTH": "REAL",
            "HISTENTROPY": "REAL",
            "AUTOCORRSIM": "REAL"
        }
        self.waveformfeatures_DB.create_table("WaveformFeatures", wvfm_features_cols)

        self.unparsable_DB = SqliteDBWrap("./database/unparsable.db")
        self.unparsable_DB.create_table("Unparsable", {"FILENAME": "TEXT PRIMARY KEY", "BUID": "TEXT"})

        self.samplingfs_DB = SqliteDBWrap("./database/samplingfs.db")
        self.samplingfs_DB.create_table("fs", {"EUID": "TEXT PRIMARY KEY", "fs": "INT"})
        return



    def receive_batch(self):
        buid = 'b' + str(uuid.uuid4())[:8]

        subbatch_progress = 0

        jsons = []
        extracted_ident = []
        unparsable = []
        fss = []
        signal_container = torch.zeros(self.subbatch_size * 8 * 5000, dtype=torch.float32)

        for xml in os.listdir(self.xml_dir):
            try:
                # Check for fundamentals: parses, RHRN (Patient ID), Rhythm Waveform, Acquisition Date and Time all exist
                tree = ET.parse(self.xml_dir + xml)
                wvfm = tree.findall('.//Waveform')[1]
                fs = int(wvfm.find("SampleBase").text)
                date_time_reformat, date, time = getFormattedDateTime(tree)
                rhrn = tree.find('.//PatientID').text
                if rhrn is None: raise Exception()
                if fs not in (250, 500): raise Exception()
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
            euid = 'g' + str(uuid.uuid4())[:8]

            # Write the waveform and place in signal container
            ecg = writeh5(tree, euid, puid, self.h5_dir)
            l = subbatch_progress * 8 * 5000
            r = (subbatch_progress + 1) * 8 * 5000
            signal_container[l:r] = ecg

            # Split the tree to de-identify
            deid_tree, identified_elements = deidentify(tree, self.identified_attr)
            extracted_ident.append([euid, puid, buid] + identified_elements)
            fss.append([euid, fs])


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
                self.samplingfs_DB.batch_write_listlists("fs", fss)

                start = prgm_time.time()
                resCL, resHE, resAC, res20flat = invoke_gpu(signal_container, subbatch_progress*8)
                print(prgm_time.time() - start)
                start = prgm_time.time()
                resAC = get_autocorr_sim(signal_container, subbatch_progress*8)
                print(prgm_time.time() - start)
                # Empty containers
                jsons = []
                unparsable = []
                extracted_ident = []
                fss = []
                subbatch_progress = 0

        self.decoder_DB.batch_write_listlists("Decoder", extracted_ident)
        self.unparsable_DB.batch_write_listlists("Unparsable", unparsable)
        self.samplingfs_DB.batch_write_listlists("fs", fss)
        if subbatch_progress > 0:
            resCL, resHE, resAC, res20flat = invoke_gpu(signal_container, subbatch_progress)

        self.decoder_DB.exit()
        self.unparsable_DB.exit()
        self.samplingfs_DB.exit()

        send_json_to_socket(jsons, self.URL)

        write_rhrn2puid_mapping(self.rhrn_puid_map, self.CIROC_PATIENT_PATH)
        generate_summary(buid, self.decoder_DB)
        return