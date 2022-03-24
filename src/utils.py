import pandas as pd
import json
import h5py
import torch
import base64
import array

up = torch.nn.Upsample(scale_factor=2, mode='linear', align_corners=False)

def send_json_to_socket(arr, url):
    """

    :param arr:
    :param url:
    :return:
    """
    return


def is_duplicate(puid, acqDate, acqTime, sqlwrapper):
    """
    Used for duplicate entry detection
    :param puid:
    :param sqlwrapper:
    :return:
    """
    res = sqlwrapper.conx.execute("SELECT * FROM Decoder WHERE PUID=:puid AND AcquisitionDate=:acqDate AND AcquisitionTime=:acqTime",
                {"puid": puid, 'acqDate':acqDate, 'acqTime':acqTime})
    if len(res.fetchall()) == 0:
        return False
    return True

def writeh5(tree, euid, puid, h5path):
    """
    Decode the waveforms in the xml, upsample if needed, save them in a contiguous tensor to h5
    :param tree: (ElementTree) of the ecg xml
    :param euid: (str) encounter id
    :param puid: (str) patient id
    :param h5path: (str) /path/to/write/h5s/to/
    :return: None, write the file to disk
    """
    rhythm_wfrm = tree.findall('.//Waveform')[1]
    rhythm_leads = rhythm_wfrm.findall("LeadData")
    # Sampling frequency
    fs = int(rhythm_wfrm.find("SampleBase").text)

    ECG_lead_dict = {}

    # Assume xml always has 8 leads
    for lead_ind in range(8):
        lead_xml = rhythm_leads[lead_ind]
        encodedStr = lead_xml.find("WaveFormData").text
        lead_ID = lead_xml.find("LeadID").text
        to_decode = base64.b64decode(encodedStr)
        T = torch.tensor(array.array('h', to_decode), dtype=torch.float32)
        # Upsample to 250 if needed
        if fs == 250:
            T = up(T.unsqueeze(0).unsqueeze(0)).flatten()
        ECG_lead_dict[lead_ID] = T

    # Create the contiguous tensor, literals acceptable here because this is standard lead and signal length
    ecg = torch.zeros(5000*8)

    for i, key in enumerate(("I", "II", "V1", "V2", "V3", "V4", "V5", "V6")):
        ecg[i*5000:(i+1)*5000] = ECG_lead_dict[key]

    h5 = h5py.File(h5path + euid + "_" + puid + ".h5", 'w')
    h5.create_dataset("ECG", data=ecg)
    h5.close()
    return


def get_rhrn2puid_mapping(CIROC_PATIENT_PATH):
    DB_csv = pd.read_csv(CIROC_PATIENT_PATH, dtype=str)
    RHRNs = list(DB_csv["RHRN"])
    PUIDs = list(DB_csv["PUID"])
    return {RHRNs[i] : PUIDs[i] for i in range(DB_csv.shape[0])}


def write_rhrn2puid_mapping(mapping, CIROC_PATIENT_PATH):
    pd.DataFrame({"RHRN": list(mapping.keys()), "PUID": list(mapping.values())},
                 columns=["RHRN", "PUID"]).to_csv(CIROC_PATIENT_PATH, index=False)
    return


def get_json_str(euid, puid, acquisitionDate, xml_str):
    """

    :param euid:
    :param puid:
    :param acquisitionDate:
    :param xml_str:
    :return:
    """
    json_str = json.dumps({
        "EUID" : euid,
        "PUID" : puid,
        "AcquisitionDate": acquisitionDate,
        "XML" : xml_str
    })
    return json_str


def deidentify(tree, identified_attr):
    """

    :param identified_attr:
    :param tree:
    :return:
    """
    identified_elements = []
    for attribute in identified_attr:
        found = tree.find(".//" + attribute)
        if found is None:
            identified_elements.append("NULL")
            continue
        identified_elements.append(found.text)
        found.text = ' '

    return tree, identified_elements


def getFormattedDateTime(tree):
    """

    :param tree:
    :return:
    """
    d = tree.find('.//AcquisitionDate').text
    t = tree.find('.//AcquisitionTime').text
    if None in (d, t):
        raise Exception("Could not find Date or Time of Acquisition")
    # Shift date string to YYYY-MM-DD
    # FORMAT YYYY-MM-DD_HH:MM:SS
    return d.split("-")[-1] + "-" + d[:5] + "_" + t, d, t
