import numpy as np
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import xml.etree.ElementTree as ET
import uuid
from random import randrange
from pydicom import dcmread
from pydicom import config

class Receive:

    def __init__(self, src, dest, BUIDS, EUIDS, PUIDS, RHRNs):
        """
        :param src: (string) path to directory of identified ECG xmls
        :param dest: (string) destination path to directory for output of de-identified .xmls and .dcms
        Dictionaries for O(1) lookup
        :param BUIDS: (dict) { buid : '', ...}
        :param EUIDS: (dict) { euid : '', ...}
        :param PUIDS: (dict) { puids : [ECG acquisition DateTime, ECG acquisition DateTime, ... for each encounter], ... }
                                       FORMAT YYYY-MM-DD_HH:MM:SS
        :param RHRNs: (dict) { rhrn : puid, ... }
        """
        self.src = src
        self.dest = dest
        if len(os.listdir(src)) == 0:
            raise Exception("Source directory given has no files to process.")

        if not Path(dest).is_dir():
            raise Exception("Destination directory is either a file or does not exist")

        self.BUIDS = BUIDS
        self.EUIDs = EUIDS
        self.PUIDs = PUIDS
        self.RHRNs = RHRNs
        self.BUID_prefixes = ['c']
        self.EUID_prefixes = ['b', 'e']
        self.PUID_prefixes = ['a', 'p']

        self.buid = self.getBUID()

        self.dest_xml = dest + "/" + self.buid + "-xml"
        os.mkdir(self.dest_xml)
        self.dest_dicom = dest + "/" + self.buid + "-DICOM"
        os.mkdir(self.dest_dicom)


        source_info = pd.read_csv('source-info.csv', header=None, index_col=0, squeeze=True).to_dict()
        self.log_info = {
            "BUID": self.buid,
            "DateTime Received": datetime.now().strftime("%b-%d-%Y %H:%M:%S"),
            "Source Details": source_info['source'],
            "Sender": source_info['sender'],
            "Receiver": source_info['receiver'],
            "Unique Site Names": '',
            "Oldest Acq Date": '',
            "Newest Acq Date": '',
            "Total .xml files": sum(1 for name in os.listdir(src) if name[-4:] == ".xml"),
            "Unique Patients": 0,
            "New PUIDs Generated": 0,
            "Not-Parsable": 0,
        }

        # De-identification parameters, update as needed. Values will replace the element. If values are set to empty
        # string opening tag is removed entirely
        # None means do not do anything - will update with another value, also prefer this element to exist in decoder.
        self.tags2replace = {
            "PatientLastName": None,
            "PatientFirstName": None,
            "LocationName": ' ',
            "DateofBirth": ' ',
            "Gender": None,
            "PatientAge": None
        }

        for elem in ET.parse('/home/rylan/PycharmProjects/forMayo/May_2019_XML/MUSE_20190315_120232_20000.xml').iter():
            if elem.tag in self.tags2replace: continue
            if elem.tag == "QRSTimesTypes": break
            tag_string = elem.tag.lower()
            if "date" in tag_string or "time" in tag_string or "name" in tag_string or "location" in tag_string:
                self.tags2replace[elem.tag] = ' '

        # The dataframe/spreadsheet that holds the removed elements. Each row a patient
        self.decoder_list = {
            "EUID": [],
            "BUID": [],
            "PUID": [],
            "RHRN": [],
        }

        for key in self.tags2replace.keys():
            self.decoder_list[key] = []

        return

    def getBUID(self):
        gen = self.get_any_UUID(self.BUID_prefixes, self.BUIDS)
        self.BUIDS[gen] = ''
        return gen

    def getPUID(self):
        gen = self.get_any_UUID(self.PUID_prefixes, self.PUIDs)
        self.PUIDs[gen] = []
        return gen

    def getEUID(self):
        gen = self.get_any_UUID(self.EUID_prefixes, self.EUIDs)
        self.EUIDs[gen] = ''
        return gen

    def get_any_UUID(self, prefixes, master_list):
        pre = prefixes[randrange(len(prefixes))]
        new_uuid = pre + str(uuid.uuid4())[:8]
        # Generate new ones until unique
        while new_uuid in master_list:
            new_uuid = pre + str(uuid.uuid4())[:8]
        return new_uuid


    def DICOM_Save(self, filename, tags2values):
        """
        :param filename: The .xml file name to wrap into a .dcm
        :param tags2values: the master dictionary containing keys (str) of xml tags to element value (str)
        :return: None, save a .dcm file following the Encapsulated PDF Information Object Definition as defined in
                 Table A.45.1.3 of DICOM standards
        """
        config.enforce_valid_values = True
        with open("./template.dcm", 'rb') as to_read:
            ds = dcmread(to_read)
        del ds.InstanceCreationDate
        del ds.InstanceCreationTime
        del ds.ContentDate
        del ds.ContentTime

        ds.BurnedInAnnotation = "NO"
        ds.DocumentTitle = filename
        ds.StudyDescription = "ECG XML"
        ds.StudyDate = tags2values["AcquisitionDate"].replace('-', str())  # YYYYMMDD
        ds.AcquisitionDateTime = tags2values["AcquisitionDate"].replace('-', str()) + tags2values["AcquisitionTime"].replace(':', str())  # YYYYMMDDHHMMSS
        ds.PatientID = tags2values["PUID"]
        ds.StudyID = tags2values["EUID"]

        if tags2values["Gender"][0] is None:
            ds.PatientSex = ''
        else:
            ds.PatientSex = tags2values["Gender"][0]

        if tags2values["PatientAge"] is None:
            ds.PatientAge = ''
        else:
            ds.PatientAge = tags2values["PatientAge"]

        ds.PatientName = tags2values["PUID"] + "_" + tags2values["EUID"]
        ds.Modality = "ECG"
        ds.EncapsulatedDocument = tags2values["xml"].encode(encoding='utf-8')
        ds.MIMETypeOfEncapsulatedDocument = "text/xml"

        # TODO remove integer random generations
        ds.StudyInstanceUID = ds.StudyInstanceUID[:-5] + "".join(str(randrange(10)) for _ in range(5))
        ds.SeriesInstanceUID = ds.SeriesInstanceUID[:-5] + "".join(str(randrange(10)) for _ in range(5))
        ds.SOPInstanceUID = ds.SOPInstanceUID[:-5] + "".join(str(randrange(10)) for _ in range(5))
        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
        ds.save_as(filename, write_like_original=True)
        return

    def getFormattedDateTime(self, tree):
        d = tree.find('.//AcquisitionDate').text
        t = tree.find('.//AcquisitionTime').text
        if None in (d, t):
            raise Exception("Could not find Date or Time of Acquisition")
        # Shift date string to YYYY-MM-DD
        # FORMAT YYYY-MM-DD_HH:MM:SS
        return d.split("-")[-1] + "-" + d[:5] + "_" + t

    def deidentifyBatch(self):
        # Generated PUID,RHRN not present in database
        absent_PUID_RHRN = []
        # .xml file names that were not able to be parsed (contain errors)
        unparsable = []
        # Acquisition dates
        acqDates = []
        # Unique patients
        uniquePatients = set()
        # Unique Site Names
        uniqueSiteNames = set()
        # List of all fs
        fs = []
        # List of all LowPassFilter values
        lpf = []

        for xml in os.listdir(self.src):
            # Ensure xml file
            if xml[-4:] != ".xml": continue
            # Attempt to parse the xml tree
            try:
                # Check for fundamentals: parses, RHRN (Patient ID), Rhythm Waveform, Acquisition Date and Time all exist
                tree = ET.parse(self.src + "/" + xml)
                wvfm = tree.findall('.//Waveform')[1]
                date_time = self.getFormattedDateTime(tree)
                rhrn = tree.find('.//PatientID').text
                if rhrn is None: raise Exception()
            except:
                print(xml + str(" was not able to be parsed, moving on"))
                unparsable.append((self.buid, xml))
                continue

            acq_date = date_time[:10]
            acq_time = date_time[11:]
            acqDates.append(acq_date)

            if not rhrn in self.RHRNs:
                puid = self.getPUID()
                self.RHRNs[rhrn] = puid
                absent_PUID_RHRN.append((puid, rhrn))
            else:
                puid = self.RHRNs[rhrn]

            if date_time in self.PUIDs[puid]:
                # We have a duplicate ECG, do not create a new encounter
                self.log_info["Duplicate ECGs"] += 1
                continue

            self.PUIDs[puid].append(date_time)
            euid = self.getEUID()

            # Add to summary containers
            uniquePatients.add(puid)
            uniqueSiteNames.add(tree.find('.//SiteName').text)
            lowpassfilter = tree.findall('.//Waveform')[1].find("LowPassFilter").text
            if lowpassfilter is not None: lpf.append(lowpassfilter)
            freq = tree.findall('.//Waveform')[1].find("SampleBase").text
            if freq is not None: fs.append(freq)

            # Complete the actual de-identification by removing set tags.
            for key in self.tags2replace.keys():
                e = tree.find('.//' + key)
                if e is None:
                    self.decoder_list[key].append(None)
                    continue
                self.decoder_list[key].append(e.text)
                if key == "PatientAge" and e.text >= '90':
                    e.text = '999'
                elif self.tags2replace[key] is not None:
                    e.text = self.tags2replace[key]

            tree.find('.//PatientID').text = puid
            self.decoder_list["PUID"].append(puid)
            tree.find('.//PatientLastName').text = puid
            tree.find('.//DateofBirth').text = self.buid
            self.decoder_list["BUID"].append(self.buid)
            tree.find('.//PatientFirstName').text = euid
            self.decoder_list["EUID"].append(euid)
            self.decoder_list["RHRN"].append(rhrn)


            # -------------------------- Writing --------------------------
            filename = self.buid + "_" + puid + "_" + euid

            # Save xml in xml dest
            tree.write(self.dest_xml + "/" + filename + ".xml", xml_declaration=True)

            # Save DICOM in DICOM dest
            self.DICOM_Save(self.dest_dicom + "/" + filename + ".dcm", {
                "AcquisitionDate": acq_date,
                "AcquisitionTime": acq_time,
                "PUID": puid,
                "Gender": tree.find('.//Gender').text,
                "PatientAge": tree.find('.//PatientAge').text,
                "BUID": self.buid,
                "EUID": euid,
                "xml": Path(self.src + "/" + xml).read_text()
            })

        # -------------------------- Summarization --------------------------
        sorted_acqDates = sorted(acqDates)
        self.log_info["Unique Site Names"] = list(uniqueSiteNames)
        self.log_info["Oldest Acq Date"] = sorted_acqDates[0]
        self.log_info["Newest Acq Date"] = sorted_acqDates[-1]
        self.log_info["Unique Patients"] = len(uniquePatients)

        # TODO update new BUID and EUID generated
        self.log_info["New PUIDs Generated"] = len(absent_PUID_RHRN)

        self.log_info["Not-Parsable"] = len(unparsable)
        self.log_info["% at 500 fs"] = round(fs.count('500') / len(fs) * 100, 3)
        self.log_info["% at 40 LPF"] = round(lpf.count('40') / len(lpf) * 100, 3)

        # -------------------------- File Writing --------------------------
        if not Path("out2").is_dir():
            os.mkdir("out2")
        if Path("out2/ECG-BUID-log.csv").is_file():
            pd.DataFrame(self.log_info).to_csv("out2/ECG-BUID-log.csv", mode='a', index=False, header=False)
        else:
            pd.DataFrame(self.log_info).to_csv("out2/ECG-BUID-log.csv", index=False)

        if Path("out2/ECG-decoder.csv").is_file():
            pd.DataFrame(self.decoder_list).to_csv("out2/ECG-decoder.csv", mode='a', index=False, header=False)
        else:
            pd.DataFrame(self.decoder_list).to_csv("out2/ECG-decoder.csv", index=False)

        if Path("out2/new-patients.csv").is_file():
            pd.DataFrame(absent_PUID_RHRN, columns=["puid", "rhrn"]).to_csv("out2/new-patients.csv", mode='a', index=False, header=False)
        else:
            pd.DataFrame(absent_PUID_RHRN, columns=["puid", "rhrn"]).to_csv("out2/new-patients.csv", index=False)

        if Path("out2/unparsable.csv").is_file():
            pd.DataFrame(unparsable, columns=["BUID", "filename"]).to_csv("out2/unparsable.csv", mode='a', index=False, header=False)
        else:
            pd.DataFrame(unparsable, columns=["BUID", "filename"]).to_csv("out2/unparsable.csv", index=False)
        return
