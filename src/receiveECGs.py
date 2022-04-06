import uuid
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

        self.subbatch_size = 5000
        self.rhrn_puid_map = get_rhrn2puid_mapping(self.CIROC_PATIENT_PATH)

        # CREATING AND OR UPDATING 5 DATABASES -- single table per database for now!!

        # -----  1 decoder.db - holding all identified information
        self.id_tags = (pd.read_csv("./src/id-tags.txt", header=None)[0]).to_list()
        self.decoder_DB = SqliteDBWrap("./database/decoder.db")
        decoder_cols = {
            "EUID": "TEXT PRIMARY KEY",
            "PUID": "TEXT",
            "BUID": "TEXT"
        }
        for col_name in self.id_tags:
            decoder_cols[col_name] = "TEXT"
        self.decoder_DB.create_table("decoder", decoder_cols)

        # -----  2 computedFeatures.db - holding extracted features from the signals for quality detection
        self.computed_features_DB = SqliteDBWrap("./database/computedFeatures.db")
        computed_features_cols = {
            "EUID_LEAD": "TEXT PRIMARY KEY",
            "BUID": "TEXT",
            "LEAD": "INT",
            "NOCHANGE20": "INT",
            "CURVELENGTH": "REAL",
            "HISTENTROPY": "REAL",
            "AUTOCORRSIM": "REAL"
        }
        self.computed_features_DB.create_table("computedFeatures", computed_features_cols)

        # -----  3 unparsable.db - write those xml filenames that could not be parsed
        self.unparsable_DB = SqliteDBWrap("./database/unparsable.db")
        self.unparsable_DB.create_table("unparsable", {"FILENAME": "TEXT PRIMARY KEY", "BUID": "TEXT"})

        # -----  4 waveformMeasurements.db - MUSE writes values pertaining to the signal within the xml, extract these
        self.measurement_tags = (pd.read_csv("./src/measurement-tags.txt", header=None)[0]).to_list()
        self.wvfm_measurements_DB = SqliteDBWrap("./database/waveformMeasurements.db")
        wvfm_measurements_cols = {
            "EUID": "TEXT PRIMARY KEY",
            "BUID": "TEXT",
            "AcquisitionDate": "TEXT",
            "fs": "INT",
            "LOWPASS": "INT",
            "HIGHPASS": "INT",
            "AC": "INT",
            "QRSNUMPY": "NDARRAY",
            "GlobalRR": "INT",
            "QTRGGR": "INT",
        }
        for col_name in self.measurement_tags:
            wvfm_measurements_cols[col_name] = "INT"
        self.wvfm_measurements_DB.create_table("waveformMeasurements", wvfm_measurements_cols)

        # -----  5 diagnosisStatements.db - Extract all physician written comments, statements, and test reasons
        self.statement_txt_DB = SqliteDBWrap("./database/diagnosisStatements.db")
        self.statement_txt_DB.create_table("diagnosisStatements", {
            "EUID": "TEXT PRIMARY KEY",
            "PUID": "TEXT",
            "BUID": "TEXT",
            "DIAGNOSIS": "TEXT",
            "ORIGINALDIAGNOSIS": "TEXT",
            "EXTRAQUESTIONS": "TEXT",
            "TESTREASON": "TEXT"
        })
        return



    def receive_batch(self):
        """

        :return:
        """
        buid = 'b' + str(uuid.uuid4())[:8]

        subbatch_progress = 0
        decoder = []
        unparsable = []
        signal_container = torch.zeros(self.subbatch_size * 8 * 5000, dtype=torch.float32)
        lead_euids = []
        statement_txt = []
        wvfm_measurements = []

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
            euid = 'g' + str(uuid.uuid4())
            lead_euids += [euid+"."+str(lead) for lead in range(8)]

            # Write the waveform and place in signal container
            ecg = writeh5(tree, euid, puid, self.h5_dir)
            l = subbatch_progress * 8 * 5000
            r = (subbatch_progress + 1) * 8 * 5000
            signal_container[l:r] = ecg

            # Get the statement texts
            statement_txt.append([euid, puid, buid] + parse_statement_text(tree))

            # QRS Measures
            wvfm_measurements.append([euid, buid, date] + parse_filters(wvfm) + parse_qrs_measurements(tree, self.measurement_tags))

            # Split the tree to de-identify
            deid_tree, identified_elements = deidentify(tree, self.id_tags)
            decoder.append([euid, puid, buid] + identified_elements)


            # Embed the UID keys for future reference
            tree.find('.//PatientID').text = puid
            tree.find('.//PatientLastName').text = puid
            tree.find('.//DateofBirth').text = buid
            tree.find('.//PatientFirstName').text = euid

            # Send json to socket
            xml_string = ET.tostring(deid_tree.getroot()).decode()


            subbatch_progress += 1
            if subbatch_progress == self.subbatch_size:
                self.decoder_DB.batch_insert(decoder)
                computed_cols = write_lead_features(signal_container, subbatch_progress, lead_euids, buid)
                self.computed_features_DB.batch_insert(iter(computed_cols))
                self.unparsable_DB.batch_insert(unparsable)
                self.wvfm_measurements_DB.batch_insert(wvfm_measurements)
                self.statement_txt_DB.batch_insert(statement_txt)

                # Empty containers
                jsons = []
                unparsable = []
                decoder = []
                wvfm_measurements = []
                lead_euids = []
                statement_txt = []
                subbatch_progress = 0
                print("Finished batch of " + str(self.subbatch_size))


        self.unparsable_DB.batch_insert(unparsable)

        if subbatch_progress > 0:
            self.decoder_DB.batch_insert(decoder)
            computed_cols = write_lead_features(signal_container, subbatch_progress, lead_euids, buid)
            self.computed_features_DB.batch_insert(iter(computed_cols))
            self.wvfm_measurements_DB.batch_insert(wvfm_measurements)
            self.statement_txt_DB.batch_insert(statement_txt)

        self.decoder_DB.exit()
        self.computed_features_DB.exit()
        self.unparsable_DB.exit()
        self.wvfm_measurements_DB.exit()
        self.statement_txt_DB.exit()

        write_rhrn2puid_mapping(self.rhrn_puid_map, self.CIROC_PATIENT_PATH)
        return